-- =============================================================================
-- VCE — MailSender Pro Veri Kalitesi Kuralları (Seed Data)
-- =============================================================================
-- Bu dosya tüm veri kalitesi kurallarını vce_dq_rules tablosuna ekler.
-- Kod değiştirmeden yeni kural eklemek için bu dosyaya INSERT yaz veya
-- doğrudan tabloya INSERT yap.
--
-- KURAL TİPLERİ:
--   threshold : sql_statement sonucu > 0 ise ihlal (COUNT(*) döndürmeli)
--   anomaly   : geçmiş 30 günlük ortalamaya göre istatistiksel sapma tespiti
--   freshness : tablonun belirli süre içinde güncellenip güncellenmediği
--   volume    : tablodaki satır sayısının beklenen aralıkta olup olmadığı
--   schema    : kolonun var olup olmadığı, tipi
--   duplicate : unique ihlali tespiti
--   custom    : herhangi bir SQL mantığı
-- =============================================================================

-- Önceki seed verilerini temizle (idempotent çalışma için)
-- UYARI: Üretimde bu satırı kaldırın!
-- DELETE FROM vce_dq_rules WHERE author IN ('system-seed');

-- ============================================================
-- GRUP 1: BAĞLANTI & ŞEMA SAĞLIĞI
-- ============================================================

INSERT INTO vce_dq_rules
    (rule_domain, rule_subdomain, dataset_name, table_name, check_type,
     sql_statement, action, description, execute_time, active_flag, author)
VALUES
(
    'schema', 'missing_tables',
    'mailsender', 'information_schema', 'threshold',
    -- Zorunlu 16 tablodan herhangi biri eksikse sonuç > 0 döner
    "SELECT COUNT(*) FROM (
        SELECT 'senders' as t UNION SELECT 'send_rules' UNION SELECT 'send_log'
        UNION SELECT 'audit_log' UNION SELECT 'users' UNION SELECT 'mail_templates'
        UNION SELECT 'suppression_list' UNION SELECT 'suppression_domains'
        UNION SELECT 'unsubscribe_tokens' UNION SELECT 'send_queue'
        UNION SELECT 'send_queue_log' UNION SELECT 'email_verify_jobs'
        UNION SELECT 'password_reset_tokens' UNION SELECT 'app_settings'
        UNION SELECT 'ses_notifications' UNION SELECT 'rate_limit_log'
    ) required
    WHERE required.t NOT IN (
        SELECT TABLE_NAME FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = 'aws_mailsender_pro_v3'
    )",
    'fail',
    'Zorunlu 16 sistem tablosundan herhangi birinin eksik olup olmadığını kontrol eder. '
    'Tablo eksikse uygulama hata verir. Bu kontrol şema migrasyonun başarısız '
    'olduğunu veya tablonun kazara silindiğini tespit eder.',
    '06:00', 1, 'system-seed'
);

-- ============================================================
-- GRUP 2: KULLANICI & GÜVENLİK
-- ============================================================

INSERT INTO vce_dq_rules
    (rule_domain, rule_subdomain, dataset_name, table_name, check_type,
     sql_statement, action, description, execute_time, active_flag, author)
VALUES
(
    'security', 'no_active_admin',
    'mailsender', 'users', 'threshold',
    -- Aktif admin yoksa 1 döner (ihlal)
    "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
     FROM aws_mailsender_pro_v3.users WHERE role='admin' AND is_active=1",
    'fail',
    'Sistemde en az bir aktif admin kullanıcısının bulunup bulunmadığını kontrol eder. '
    'Aktif admin yoksa kimse ayarlara erişemez, yeni kullanıcı ekleyemez. '
    'Genellikle yanlışlıkla tüm adminlerin devre dışı bırakıldığında tetiklenir.',
    '06:00', 1, 'system-seed'
),
(
    'security', 'brute_force_detection',
    'mailsender', 'audit_log', 'threshold',
    -- Son 1 saatte 50'den fazla başarısız login denemesi: brute force işareti
    "SELECT CASE WHEN COUNT(*) > 50 THEN COUNT(*) - 50 ELSE 0 END
     FROM aws_mailsender_pro_v3.audit_log
     WHERE action = 'login_failed'
       AND created_at >= NOW() - INTERVAL 1 HOUR",
    'warn',
    'Son 1 saatte 50\'den fazla başarısız giriş denemesi yapılıp yapılmadığını kontrol eder. '
    'Bu bir brute force saldırısının göstergesi olabilir. Tetiklenirse ilgili IP '
    'manuel olarak incelenmeli, gerekirse rate limiting sıkılaştırılmalıdır.',
    '06:00', 1, 'system-seed'
),
(
    'security', 'orphan_reset_tokens',
    'mailsender', 'password_reset_tokens', 'threshold',
    -- Süresi dolmuş ama kullanılmamış 1000'den fazla token: temizlik gerekiyor
    "SELECT CASE WHEN COUNT(*) > 1000 THEN COUNT(*) - 1000 ELSE 0 END
     FROM aws_mailsender_pro_v3.password_reset_tokens
     WHERE used = 0 AND expires_at < NOW()",
    'warn',
    'Süresi dolmuş ama kullanılmamış şifre sıfırlama tokenlarının sayısını kontrol eder. '
    '1000\'i geçerse remediation DAG\'ının düzgün çalışmadığının işaretidir. '
    'Bu tokenlar disk/bellek kullanımını artırır ve tablonun büyümesine neden olur.',
    '06:00', 1, 'system-seed'
),
(
    'security', 'inactive_users_with_access',
    'mailsender', 'users', 'threshold',
    -- 90 günden fazla login olmamış aktif kullanıcılar: zombie hesap riski
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.users
     WHERE is_active = 1
       AND last_login < NOW() - INTERVAL 90 DAY
       AND last_login IS NOT NULL",
    'warn',
    '90 günden fazla sisteme giriş yapmamış aktif kullanıcı hesaplarını tespit eder. '
    'Bu "zombie hesaplar" güvenlik riski oluşturur: ele geçirilirse fark edilmesi '
    'zordur. Tespit edilen hesaplar için deaktivasyonu değerlendirin.',
    '06:00', 1, 'system-seed'
);

-- ============================================================
-- GRUP 3: GÖNDERİM LOGU (SEND_LOG)
-- ============================================================

INSERT INTO vce_dq_rules
    (rule_domain, rule_subdomain, dataset_name, table_name, check_type,
     sql_statement, action, description, execute_time, active_flag, author)
VALUES
(
    'send_log', 'failed_ratio_threshold',
    'mailsender', 'send_log', 'threshold',
    -- Son 24 saatte failed oranı %30'u geçerse ihlal
    -- Toplam 10'dan az gönderimde kontrol yapma (istatistiksel anlamsızlık)
    "SELECT CASE
        WHEN total < 10 THEN 0
        WHEN failed_count / total > 0.30 THEN ROUND(failed_count / total * 100, 2)
        ELSE 0
     END as violation
     FROM (
         SELECT COUNT(*) as total,
                SUM(status='failed') as failed_count
         FROM aws_mailsender_pro_v3.send_log
         WHERE sent_at >= NOW() - INTERVAL 24 HOUR
     ) stats",
    'fail',
    'Son 24 saatteki gönderim başarısızlık oranının %30\'u geçip geçmediğini kontrol eder. '
    '%30 üzeri başarısızlık, SMTP sunucu sorunu, kötü e-posta listesi kalitesi veya '
    'spam engellemesi gibi ciddi bir probleme işaret eder. '
    'Sonuç değeri başarısızlık yüzdesi × 100 olarak döner.',
    '06:00', 1, 'system-seed'
),
(
    'send_log', 'failed_ratio_anomaly',
    'mailsender', 'send_log', 'anomaly',
    -- Anomali kontrolü: bugünkü failed sayısı geçmiş 30 güne göre istatistiksel sapma
    "SELECT SUM(status='failed') as failed_count
     FROM aws_mailsender_pro_v3.send_log
     WHERE DATE(sent_at) = CURDATE()",
    'warn',
    'Bugünkü başarısız gönderim sayısının geçmiş 30 günlük ortalamadan istatistiksel '
    'olarak sapıp sapmadığını kontrol eder (Z-skoru > 3 ise anomali). '
    'Sabit eşikten farklı olarak trafik hacmine göre dinamik uyum sağlar: '
    'düşük trafikli günlerde duyarlılık artar, yüksek trafikli günlerde azalır.',
    '06:00', 1, 'system-seed'
),
(
    'send_log', 'null_critical_fields',
    'mailsender', 'send_log', 'threshold',
    -- recipient veya status NULL olan kayıtlar: veri bütünlüğü ihlali
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_log
     WHERE recipient IS NULL OR status IS NULL OR sender_id IS NULL",
    'fail',
    'send_log tablosunda recipient, status veya sender_id alanlarının NULL olduğu '
    'kayıtları tespit eder. Bu alanlar zorunlu iş mantığı gereksinimidir: '
    'NULL recipient ile teslimat takibi yapılamaz, NULL status ile '
    'başarı/hata analizi yapılamaz.',
    '06:00', 1, 'system-seed'
),
(
    'send_log', 'spam_risk_same_recipient',
    'mailsender', 'send_log', 'threshold',
    -- Son 1 saatte aynı adrese 10'dan fazla gönderim: spam riski
    "SELECT COUNT(*) FROM (
         SELECT recipient, COUNT(*) as cnt
         FROM aws_mailsender_pro_v3.send_log
         WHERE sent_at >= NOW() - INTERVAL 1 HOUR
           AND status = 'sent'
         GROUP BY recipient
         HAVING cnt > 10
     ) spam_candidates",
    'warn',
    'Son 1 saat içinde aynı e-posta adresine 10\'dan fazla başarılı gönderim yapılıp '
    'yapılmadığını kontrol eder. Bu bir uygulama hatasının (sonsuz döngü, çift tetikleme) '
    'veya kötü niyetli kullanımın işareti olabilir. '
    'Tespit edilen adresleri audit_log\'dan inceleyerek kaynağı bulun.',
    '06:00', 1, 'system-seed'
),
(
    'send_log', 'daily_volume',
    'mailsender', 'send_log', 'anomaly',
    -- Bugünkü toplam gönderim sayısı (anomali kontrolü için)
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_log WHERE DATE(sent_at) = CURDATE()",
    'warn',
    'Bugünkü toplam gönderim hacminin geçmiş 30 günlük normale göre anormal '
    'derecede düşük veya yüksek olup olmadığını kontrol eder. '
    'Anormal düşük: sistem durdu mu? Anormal yüksek: istenmeyen toplu gönderim mi?',
    '06:00', 1, 'system-seed'
),
(
    'send_log', 'freshness',
    'mailsender', 'send_log', 'freshness',
    -- Son 24 saatte hiç gönderim yoksa tablo "stale" (bayat)
    "SELECT CASE
        WHEN MAX(sent_at) < NOW() - INTERVAL 24 HOUR OR MAX(sent_at) IS NULL
        THEN 1 ELSE 0
     END FROM aws_mailsender_pro_v3.send_log",
    'warn',
    'send_log tablosuna son 24 saat içinde en az bir kayıt eklenip eklenmediğini '
    'kontrol eder. Aktif bir sistemde en az bir gönderim beklenir. '
    'Hiç gönderim yoksa worker durdurulmuş veya kuyruk boş olabilir.',
    '06:00', 1, 'system-seed'
);

-- ============================================================
-- GRUP 4: SUPPRESSION LİSTESİ
-- ============================================================

INSERT INTO vce_dq_rules
    (rule_domain, rule_subdomain, dataset_name, table_name, check_type,
     sql_statement, action, description, execute_time, active_flag, author)
VALUES
(
    'suppression', 'violation_critical',
    'mailsender', 'send_log', 'threshold',
    -- Suppression listesindeki bir adrese başarıyla gönderim yapılmış: kritik ihlal
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_log sl
     JOIN aws_mailsender_pro_v3.suppression_list sp ON sp.email = sl.recipient
     WHERE sl.status = 'sent'
       AND sl.sent_at >= NOW() - INTERVAL 24 HOUR",
    'fail',
    'Son 24 saat içinde suppression listesindeki bir e-posta adresine başarılı '
    'gönderim yapılıp yapılmadığını kontrol eder. Bu çok kritik bir ihlaldir: '
    'bounce veya complaint suppression ihlali AWS SES hesabının askıya alınmasına, '
    'unsubscribe ihlali ise yasal sorunlara (GDPR, CAN-SPAM) yol açabilir.',
    '06:00', 1, 'system-seed'
),
(
    'suppression', 'daily_growth_anomaly',
    'mailsender', 'suppression_list', 'anomaly',
    -- Bugün eklenen suppression sayısı (anomali kontrolü)
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.suppression_list
     WHERE DATE(created_at) = CURDATE()",
    'warn',
    'Bugün suppression listesine eklenen adres sayısının geçmiş 30 günlük '
    'normale göre anormal derecede yüksek olup olmadığını kontrol eder. '
    'Anormal yüksek suppression genellikle kötü liste kalitesinin veya '
    'gönderim stratejisi sorunlarının işaretidir.',
    '06:00', 1, 'system-seed'
),
(
    'suppression', 'unsubscribe_integrity',
    'mailsender', 'unsubscribe_tokens', 'threshold',
    -- Kullanılmış unsubscribe token var ama suppression listesinde yok: tutarsızlık
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.unsubscribe_tokens ut
     WHERE ut.used = 1
       AND NOT EXISTS (
           SELECT 1 FROM aws_mailsender_pro_v3.suppression_list sl
           WHERE sl.email = ut.email AND sl.reason = 'unsubscribe'
       )
       AND ut.created_at >= NOW() - INTERVAL 7 DAY",
    'warn',
    'Kullanılmış (used=1) bir unsubscribe token\'ının, ilgili e-posta adresinin '
    'suppression listesine eklenmemiş olduğu durumları tespit eder. '
    'Bu bir uygulama katmanı hatasıdır: unsubscribe işlemi tamamlanmış ama '
    'suppression kaydı oluşturulmamış. Adres yeniden mailing listesinde kalır.',
    '06:00', 1, 'system-seed'
),
(
    'suppression', 'domain_blacklist_check',
    'mailsender', 'send_log', 'threshold',
    -- Suppression domain listesindeki domainlere gönderim yapılmış mı?
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_log sl
     JOIN aws_mailsender_pro_v3.suppression_domains sd ON sl.recipient LIKE CONCAT('%@', sd.domain)
     WHERE sl.status = 'sent'
       AND sl.sent_at >= NOW() - INTERVAL 24 HOUR",
    'fail',
    'Suppression domain listesindeki (suppression_domains tablosu) bir domain\'e '
    'ait adreslere son 24 saatte başarılı gönderim yapılıp yapılmadığını kontrol eder. '
    'Domain bazlı suppression genellikle toplu bounce veya complaint veren '
    'kurumsal e-posta sağlayıcıları için kullanılır.',
    '06:00', 1, 'system-seed'
);

-- ============================================================
-- GRUP 5: SEND QUEUE (KUYRUK)
-- ============================================================

INSERT INTO vce_dq_rules
    (rule_domain, rule_subdomain, dataset_name, table_name, check_type,
     sql_statement, action, description, execute_time, active_flag, author)
VALUES
(
    'queue', 'stuck_running',
    'mailsender', 'send_queue', 'threshold',
    -- 6+ saattir 'running' durumda olan ve ilerlemeyen görevler
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_queue
     WHERE status = 'running'
       AND started_at < NOW() - INTERVAL 6 HOUR
       AND (total_count = 0 OR current_offset < total_count)",
    'warn',
    'Kuyruktaki gönderim görevlerinin 6 saatten uzun süre "running" durumunda '
    'kalıp kalmadığını kontrol eder. Takılı görev genellikle worker crash, '
    'SMTP zaman aşımı veya bellek sorununu işaret eder. '
    'Tespit edilirse ilgili queue ID elle incelenmeli, gerekirse iptal edilmelidir.',
    '06:00', 1, 'system-seed'
),
(
    'queue', 'count_inconsistency',
    'mailsender', 'send_queue', 'threshold',
    -- Tamamlanmış görevlerde total_count = sent + failed + skipped olmalı
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_queue
     WHERE status = 'done'
       AND total_count > 0
       AND total_count != (sent_count + failed_count + skipped_count)",
    'fail',
    'Tamamlanmış (done) kuyruktaki görevlerde total_count alanının '
    'sent_count + failed_count + skipped_count toplamına eşit olup olmadığını '
    'kontrol eder. Eşit değilse bazı e-postalar işlenmeden atlanmış '
    'veya sayaçlar güncellenmemiş demektir — veri bütünlüğü ihlali.',
    '06:00', 1, 'system-seed'
),
(
    'queue', 'overdue_pending',
    'mailsender', 'send_queue', 'threshold',
    -- next_run_at geçmiş olan pending görevler: scheduler çalışmıyor olabilir
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_queue
     WHERE status = 'pending'
       AND next_run_at IS NOT NULL
       AND next_run_at < NOW() - INTERVAL 30 MINUTE",
    'warn',
    'Başlama zamanı (next_run_at) geçmiş ama hâlâ "pending" durumunda olan '
    'kuyruk görevlerini tespit eder. 30 dakikadan fazla gecikme varsa '
    'Airflow worker durmuş, MySQL bağlantısı kesilmiş veya scheduler devre dışı '
    'kalmış olabilir.',
    '06:00', 1, 'system-seed'
),
(
    'queue', 'ab_ratio_invalid',
    'mailsender', 'send_queue', 'threshold',
    -- A/B test aktif ama ratio 0-100 dışında: mantık hatası
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_queue
     WHERE ab_test = 1
       AND (ab_ratio < 0 OR ab_ratio > 100)",
    'fail',
    'A/B test aktif olan kuyruk görevlerinin ab_ratio değerinin 0-100 arasında '
    'olup olmadığını kontrol eder. Geçersiz oran (örn: -5 veya 150) uygulama '
    'katmanında hatalı hesaplamaya yol açar ve gönderimler yanlış gruba atanır.',
    '06:00', 1, 'system-seed'
),
(
    'queue', 'log_count_mismatch',
    'mailsender', 'send_queue', 'threshold',
    -- send_queue.sent_count ile send_queue_log'daki gerçek satır sayısı uyuşmuyor mu?
    "SELECT COUNT(*) FROM (
         SELECT q.id
         FROM aws_mailsender_pro_v3.send_queue q
         LEFT JOIN (
             SELECT queue_id, SUM(status='sent') as log_sent
             FROM aws_mailsender_pro_v3.send_queue_log
             GROUP BY queue_id
         ) ql ON ql.queue_id = q.id
         WHERE q.status IN ('running','done')
           AND ABS(q.sent_count - COALESCE(ql.log_sent, 0)) > 5
     ) mismatch",
    'warn',
    'send_queue tablosundaki sent_count sayacı ile send_queue_log\'daki gerçek '
    'başarılı gönderim sayısı arasında 5\'ten fazla fark olup olmadığını kontrol eder. '
    'Küçük farklar race condition nedeniyle olabilir (5 tolerans bu yüzden), '
    'büyük farklar ise kayıt tutma sorununu gösterir.',
    '06:00', 1, 'system-seed'
);

-- ============================================================
-- GRUP 6: E-POSTA DOĞRULAMA İŞLERİ
-- ============================================================

INSERT INTO vce_dq_rules
    (rule_domain, rule_subdomain, dataset_name, table_name, check_type,
     sql_statement, action, description, execute_time, active_flag, author)
VALUES
(
    'verify', 'stuck_jobs',
    'mailsender', 'email_verify_jobs', 'threshold',
    -- 12+ saattir running durumda olan doğrulama işleri
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.email_verify_jobs
     WHERE status = 'running'
       AND started_at < NOW() - INTERVAL 12 HOUR",
    'warn',
    'E-posta doğrulama işlerinin (email_verify_jobs) 12 saatten uzun süre '
    '"running" durumunda kalıp kalmadığını kontrol eder. SMTP timeout, '
    'MX lookup başarısızlığı veya thread pool tıkanması nedeniyle takılabilirler. '
    'Tespit edilirse job elle iptal edilmeli ve hata logları incelenmelidir.',
    '06:00', 1, 'system-seed'
),
(
    'verify', 'processed_exceeds_total',
    'mailsender', 'email_verify_jobs', 'threshold',
    -- processed_count > total_count: sayaç mantık hatası
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.email_verify_jobs
     WHERE processed_count > total_count AND total_count > 0",
    'fail',
    'E-posta doğrulama işlerinde işlenen (processed_count) kayıt sayısının '
    'toplam kayıt sayısını (total_count) aşıp aşmadığını kontrol eder. '
    'Bu matematiksel bir imkânsızlıktır — sayaç güncellemesinde hata var demektir. '
    'İlerleme çubukları ve tahmin hesapları bu değerden beslendiği için '
    'kullanıcıya yanlış bilgi gösterilir.',
    '06:00', 1, 'system-seed'
),
(
    'verify', 'low_valid_rate',
    'mailsender', 'email_verify_jobs', 'threshold',
    -- Son tamamlanan işlerde valid oranı %20'nin altındaysa: liste kalitesi çok kötü
    "SELECT COUNT(*) FROM (
         SELECT id,
                valid_count / NULLIF(total_count, 0) as valid_rate
         FROM aws_mailsender_pro_v3.email_verify_jobs
         WHERE status = 'done'
           AND finished_at >= NOW() - INTERVAL 7 DAY
           AND total_count >= 100
         HAVING valid_rate < 0.20
     ) bad_jobs",
    'warn',
    'Son 7 günde tamamlanan doğrulama işlerinden total_count >= 100 olanların '
    'geçerli e-posta oranının %20\'nin altında olup olmadığını kontrol eder. '
    '%20 altı liste kalitesi gönderim itibarını ciddi şekilde olumsuz etkiler. '
    'Bu tür listelerin gönderimde kullanılması önerilmez.',
    '06:00', 1, 'system-seed'
);

-- ============================================================
-- GRUP 7: SENDER (GÖNDERİCİ) KALİTESİ
-- ============================================================

INSERT INTO vce_dq_rules
    (rule_domain, rule_subdomain, dataset_name, table_name, check_type,
     sql_statement, action, description, execute_time, active_flag, author)
VALUES
(
    'senders', 'no_active_sender',
    'mailsender', 'senders', 'threshold',
    -- Aktif gönderici yoksa sistem gönderim yapamaz
    "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
     FROM aws_mailsender_pro_v3.senders WHERE is_active = 1",
    'fail',
    'Sistemde en az bir aktif gönderici (senders tablosu) olup olmadığını kontrol eder. '
    'Aktif gönderici yoksa hiçbir gönderim yapılamaz, tüm kuyruk görevleri başarısız olur. '
    'Genellikle yanlışlıkla tüm gönderilerin devre dışı bırakılmasında tetiklenir.',
    '06:00', 1, 'system-seed'
),
(
    'senders', 'smtp_incomplete_config',
    'mailsender', 'senders', 'threshold',
    -- SMTP göndericide eksik zorunlu alan var mı?
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.senders
     WHERE sender_mode = 'smtp'
       AND is_active = 1
       AND (smtp_server IS NULL OR smtp_port IS NULL
            OR username IS NULL OR password IS NULL)",
    'warn',
    'Aktif SMTP gönderimcilerin zorunlu konfigürasyon alanlarından (smtp_server, '
    'smtp_port, username, password) herhangi birinin NULL olup olmadığını kontrol eder. '
    'Eksik konfigürasyon o gönderici üzerinden gönderim yapılmaya çalışıldığında '
    'bağlantı hatasına yol açar.',
    '06:00', 1, 'system-seed'
),
(
    'senders', 'ses_incomplete_config',
    'mailsender', 'senders', 'threshold',
    -- SES gönderimcide AWS key eksik mi?
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.senders
     WHERE sender_mode = 'ses'
       AND is_active = 1
       AND (aws_access_key IS NULL OR aws_secret_key IS NULL OR aws_region IS NULL)",
    'warn',
    'Aktif AWS SES gönderimcilerin aws_access_key, aws_secret_key veya aws_region '
    'alanlarından herhangi birinin NULL olup olmadığını kontrol eder. '
    'Eksik AWS kimlik bilgileri "AuthFailure" hatasına yol açar ve '
    'tüm SES gönderimler başarısız olur.',
    '06:00', 1, 'system-seed'
),
(
    'senders', 'warmup_without_daily_limit',
    'mailsender', 'senders', 'threshold',
    -- Warmup aktif ama daily_limit=0: warmup plansız çalışır
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.senders
     WHERE warmup_enabled = 1
       AND daily_limit = 0
       AND is_active = 1",
    'warn',
    'IP warmup (ısınma) aktif olan gönderimcilerin daily_limit değerinin 0 olup '
    'olmadığını kontrol eder. Warmup\'ın amacı günlük gönderim miktarını kademeli '
    'artırmaktır — daily_limit=0 (sınırsız) ile warmup planı anlamsız hale gelir '
    've IP itibarı zarar görebilir.',
    '06:00', 1, 'system-seed'
),
(
    'senders', 'high_failure_rate',
    'mailsender', 'send_log', 'threshold',
    -- Son 24 saatte herhangi bir sender'ın başarısızlık oranı %50'yi geçiyor mu?
    "SELECT COUNT(*) FROM (
         SELECT sender_id,
                SUM(status='failed') / COUNT(*) as fail_rate
         FROM aws_mailsender_pro_v3.send_log
         WHERE sent_at >= NOW() - INTERVAL 24 HOUR
         GROUP BY sender_id
         HAVING fail_rate > 0.50 AND COUNT(*) >= 10
     ) bad_senders",
    'warn',
    'Son 24 saatte en az 10 gönderim yapan herhangi bir gönderimcinin başarısızlık '
    'oranının %50\'yi aşıp aşmadığını kontrol eder. '
    'Tek bir gönderimcinin yüksek hata oranı, o gönderimcinin SMTP/SES '
    'konfigürasyon sorununu veya IP engelini işaret eder.',
    '06:00', 1, 'system-seed'
);

-- ============================================================
-- GRUP 8: REFERANS BÜTÜNLÜĞÜ (FK)
-- ============================================================

INSERT INTO vce_dq_rules
    (rule_domain, rule_subdomain, dataset_name, table_name, check_type,
     sql_statement, action, description, execute_time, active_flag, author)
VALUES
(
    'integrity', 'send_log_orphan_sender',
    'mailsender', 'send_log', 'threshold',
    -- send_log'da var olan ama senders tablosunda olmayan sender_id
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_log sl
     LEFT JOIN aws_mailsender_pro_v3.senders s ON s.id = sl.sender_id
     WHERE s.id IS NULL",
    'fail',
    'send_log tablosundaki sender_id değerlerine karşılık gelen kaydın senders '
    'tablosunda bulunup bulunmadığını kontrol eder. MySQL\'de FOREIGN KEY '
    'kısıtlaması açık olduğunda bu teorik olarak imkânsızdır, ancak '
    'FK\'nın devre dışı bırakılması veya doğrudan DB manipülasyonu durumunda '
    'orphan kayıtlar oluşabilir.',
    '06:00', 1, 'system-seed'
),
(
    'integrity', 'queue_log_orphan_queue',
    'mailsender', 'send_queue_log', 'threshold',
    -- send_queue_log'da var olan ama send_queue'da olmayan queue_id
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_queue_log ql
     LEFT JOIN send_queue q ON q.id = ql.queue_id
     WHERE q.id IS NULL",
    'fail',
    'send_queue_log tablosundaki queue_id değerlerine karşılık gelen kaydın '
    'send_queue tablosunda bulunup bulunmadığını kontrol eder. '
    'Bu genellikle bir kuyruğun log kayıtları silinmeden doğrudan silinmesi '
    'durumunda oluşur. JOIN sorguları bu orphan kayıtlar nedeniyle hatalı '
    'sonuç döndürebilir.',
    '06:00', 1, 'system-seed'
),
(
    'integrity', 'duplicate_suppression_email',
    'mailsender', 'suppression_list', 'duplicate',
    -- suppression_list'te aynı email'den birden fazla kayıt: UNIQUE kısıt ihlali
    "SELECT COUNT(*) FROM (
         SELECT email, COUNT(*) as cnt
         FROM aws_mailsender_pro_v3.suppression_list
         GROUP BY email
         HAVING cnt > 1
     ) dupes",
    'fail',
    'suppression_list tablosunda aynı e-posta adresinin birden fazla kayıt içerip '
    'içermediğini kontrol eder. UNIQUE KEY uk_email kısıtlaması normalde bunu '
    'engeller, ancak FK devre dışıyken veya doğrudan import durumunda tekrar '
    'edebilir. Tekrarlı kayıt, suppression kontrolünü güvenilmez kılar.',
    '06:00', 1, 'system-seed'
),
(
    'integrity', 'ses_orphan_sender',
    'mailsender', 'ses_notifications', 'threshold',
    -- ses_notifications'da var olan ama senders'da olmayan sender_id
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.ses_notifications sn
     LEFT JOIN aws_mailsender_pro_v3.senders s ON s.id = sn.sender_id
     WHERE sn.sender_id IS NOT NULL AND s.id IS NULL",
    'warn',
    'ses_notifications tablosundaki sender_id değerlerine karşılık gelen kaydın '
    'senders tablosunda bulunup bulunmadığını kontrol eder. '
    'Orphan SES bildirim kaydı, bounce/complaint analizinde o bildirimin '
    'hangi gönderimciye ait olduğunun belirlenememesine neden olur.',
    '06:00', 1, 'system-seed'
);

-- ============================================================
-- GRUP 9: FRESHNESS & VOLUME
-- ============================================================

INSERT INTO vce_dq_rules
    (rule_domain, rule_subdomain, dataset_name, table_name, check_type,
     sql_statement, action, description, execute_time, active_flag, author)
VALUES
(
    'freshness', 'audit_log_stale',
    'mailsender', 'audit_log', 'freshness',
    -- audit_log'a 48 saattir kayıt eklenmemişse uygulama log almıyor olabilir
    "SELECT CASE
        WHEN MAX(created_at) < NOW() - INTERVAL 48 HOUR OR MAX(created_at) IS NULL
        THEN 1 ELSE 0
     END FROM aws_mailsender_pro_v3.audit_log",
    'warn',
    'audit_log tablosuna son 48 saat içinde en az bir kayıt eklenip eklenmediğini '
    'kontrol eder. Aktif bir sistemde kullanıcı girişi, gönderim başlatma gibi '
    'aksiyonlar audit logu tetikler. 48 saatlik sessizlik, audit log mekanizmasının '
    'durduğunu veya uygulama sunucusunun kapandığını işaret edebilir.',
    '06:00', 1, 'system-seed'
),
(
    'volume', 'suppression_list_empty',
    'mailsender', 'suppression_list', 'volume',
    -- Yeni kurulumda suppression listesi boş olabilir ama production'da boş olmamalı
    "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM aws_mailsender_pro_v3.suppression_list",
    'warn',
    'suppression_list tablosunun tamamen boş olup olmadığını kontrol eder. '
    'Production ortamında her zaman en az birkaç unsubscribe veya bounce kaydı '
    'bulunması beklenir. Tamamen boş liste, suppression mekanizmasının '
    'devre dışı bırakıldığını veya tablonun yanlışlıkla temizlendiğini gösterebilir.',
    '06:00', 1, 'system-seed'
),
(
    'volume', 'ses_notification_volume',
    'mailsender', 'ses_notifications', 'anomaly',
    -- Bugünkü SES bounce/complaint sayısı anormal derecede yüksek mi?
    "SELECT COUNT(*) FROM aws_mailsender_pro_v3.ses_notifications
     WHERE DATE(received_at) = CURDATE()
       AND notif_type IN ('Bounce', 'Complaint')",
    'warn',
    'Bugün alınan SES Bounce ve Complaint bildirimlerinin geçmiş 30 günlük '
    'ortalamaya göre anormal derecede yüksek olup olmadığını kontrol eder. '
    'Ani bounce/complaint artışı, kötü liste kalitesi, IP itibar sorunu '
    'veya içerik filtrelemesi nedeniyle tetiklenebilir.',
    '06:00', 1, 'system-seed'
);
