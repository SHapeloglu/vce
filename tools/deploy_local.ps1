# =============================================================================
# tools/deploy_local.ps1
# VCE — Windows Yerel Deploy Script'i
# =============================================================================
#
# CI/CD'nin SSH ile Airflow'a ulaşamadığı durumlarda (örn: Windows localhost)
# bu script'i manuel veya görev zamanlayıcısıyla çalıştırabilirsin.
#
# KULLANIM:
#   # PowerShell'de:
#   .\tools\deploy_local.ps1
#
#   # Sadece belirli dosyaları kopyala:
#   .\tools\deploy_local.ps1 -OnlyOperators
#   .\tools\deploy_local.ps1 -OnlyDAGs
#
#   # Test modunda çalıştır (kopyalamaz, sadece ne yapacağını gösterir):
#   .\tools\deploy_local.ps1 -DryRun
#
# VARSAYILAN YOLLAR:
#   Kaynak : Bu script'in bulunduğu repo'nun kökü
#   Hedef  : C:\Users\yeliz\Desktop\apache-airflow-docker\dags
#
# =============================================================================

param(
    [switch]$DryRun,          # Kopyalamadan sadece göster
    [switch]$OnlyOperators,   # Sadece operators/ kopyala
    [switch]$OnlyDAGs,        # Sadece DAG dosyalarını kopyala
    [switch]$RestartDocker,   # Deploy sonrası Docker'ı yeniden başlat
    [string]$AirflowDagsPath  # Özel hedef yol
)

# ── Ayarlar ──────────────────────────────────────────────────────────────────

$RepoRoot     = Split-Path -Parent (Split-Path -Parent $PSCommandPath)
$OperatorsDir = Join-Path $RepoRoot "operators"
$DagsDir      = Join-Path $RepoRoot "dags"

# Hedef Airflow dags klasörü
if ($AirflowDagsPath) {
    $TargetDagsPath = $AirflowDagsPath
} else {
    $TargetDagsPath = "C:\Users\yeliz\Desktop\apache-airflow-docker\dags"
}

$TargetOperatorsPath = Join-Path $TargetDagsPath "operators"

# ── Renkli çıktı yardımcıları ─────────────────────────────────────────────────

function Write-OK   { param($msg) Write-Host "  ✅ $msg" -ForegroundColor Green }
function Write-FAIL { param($msg) Write-Host "  ❌ $msg" -ForegroundColor Red }
function Write-INFO { param($msg) Write-Host "  ℹ️  $msg" -ForegroundColor Cyan }
function Write-WARN { param($msg) Write-Host "  ⚠️  $msg" -ForegroundColor Yellow }

# ── Başlangıç ─────────────────────────────────────────────────────────────────

Write-Host ""
Write-Host "════════════════════════════════════════════════" -ForegroundColor Blue
Write-Host "  VCE — Yerel Deploy Script'i" -ForegroundColor Blue
Write-Host "  $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Blue
Write-Host "════════════════════════════════════════════════" -ForegroundColor Blue
Write-Host ""

if ($DryRun) {
    Write-WARN "DRY RUN modu — dosyalar kopyalanmayacak"
    Write-Host ""
}

Write-INFO "Kaynak   : $RepoRoot"
Write-INFO "Hedef    : $TargetDagsPath"
Write-Host ""

# ── Hedef klasör kontrolü ─────────────────────────────────────────────────────

if (-not (Test-Path $TargetDagsPath)) {
    Write-FAIL "Hedef klasör bulunamadı: $TargetDagsPath"
    Write-WARN "Airflow çalışıyor mu? Doğru yolu giriniz."
    Write-WARN "Kullanım: .\deploy_local.ps1 -AirflowDagsPath 'C:\...\dags'"
    exit 1
}

if (-not $DryRun) {
    # operators/ alt klasörünü oluştur
    if (-not (Test-Path $TargetOperatorsPath)) {
        New-Item -ItemType Directory -Path $TargetOperatorsPath | Out-Null
        Write-OK "operators/ klasörü oluşturuldu."
    }
}

# ── Kopyalanacak dosyalar ──────────────────────────────────────────────────────

$OperatorFiles = @(
    "vce_operators.py",
    "vce_operators_extended.py",
    "vce_operators_ml_lifecycle.py"
)

$DAGFiles = @(
    "mailsender_vce_main.py",
    "mailsender_vce_remediation.py",
    "mailsender_vce_partition_manager.py",
    "mailsender_vce_ml_lifecycle.py"
)

$CopiedCount = 0
$SkippedCount = 0
$ErrorCount = 0

# ── Operatörleri kopyala ──────────────────────────────────────────────────────

if (-not $OnlyDAGs) {
    Write-Host "📁 Operatörler kopyalanıyor..." -ForegroundColor White
    foreach ($file in $OperatorFiles) {
        $src = Join-Path $OperatorsDir $file
        $dst = Join-Path $TargetOperatorsPath $file

        if (-not (Test-Path $src)) {
            Write-WARN "$file — kaynak bulunamadı, atlandı"
            $SkippedCount++
            continue
        }

        # Değişiklik var mı?
        $changed = $true
        if (Test-Path $dst) {
            $srcHash = (Get-FileHash $src -Algorithm MD5).Hash
            $dstHash = (Get-FileHash $dst -Algorithm MD5).Hash
            if ($srcHash -eq $dstHash) {
                $changed = $false
            }
        }

        if (-not $changed) {
            Write-INFO "$file — değişmemiş, atlandı"
            $SkippedCount++
        } elseif ($DryRun) {
            Write-OK "$file — kopyalanacak (DRY RUN)"
            $CopiedCount++
        } else {
            try {
                Copy-Item -Path $src -Destination $dst -Force
                Write-OK "$file — kopyalandı"
                $CopiedCount++
            } catch {
                Write-FAIL "$file — kopyalama hatası: $_"
                $ErrorCount++
            }
        }
    }
    Write-Host ""
}

# ── DAG dosyalarını kopyala ───────────────────────────────────────────────────

if (-not $OnlyOperators) {
    Write-Host "📁 DAG dosyaları kopyalanıyor..." -ForegroundColor White
    foreach ($file in $DAGFiles) {
        $src = Join-Path $DagsDir $file
        $dst = Join-Path $TargetDagsPath $file

        if (-not (Test-Path $src)) {
            Write-WARN "$file — kaynak bulunamadı, atlandı"
            $SkippedCount++
            continue
        }

        $changed = $true
        if (Test-Path $dst) {
            $srcHash = (Get-FileHash $src -Algorithm MD5).Hash
            $dstHash = (Get-FileHash $dst -Algorithm MD5).Hash
            if ($srcHash -eq $dstHash) { $changed = $false }
        }

        if (-not $changed) {
            Write-INFO "$file — değişmemiş, atlandı"
            $SkippedCount++
        } elseif ($DryRun) {
            Write-OK "$file — kopyalanacak (DRY RUN)"
            $CopiedCount++
        } else {
            try {
                Copy-Item -Path $src -Destination $dst -Force
                Write-OK "$file — kopyalandı"
                $CopiedCount++
            } catch {
                Write-FAIL "$file — kopyalama hatası: $_"
                $ErrorCount++
            }
        }
    }
    Write-Host ""
}

# ── Deploy Manifest ───────────────────────────────────────────────────────────

if (-not $DryRun -and $CopiedCount -gt 0) {
    $ManifestPath = Join-Path $TargetDagsPath "VCE_DEPLOY_MANIFEST.txt"
    $GitHash = ""
    try {
        $GitHash = git -C $RepoRoot rev-parse --short HEAD 2>$null
    } catch {}

    $Manifest = @"
VCE Deploy Manifest
===================
Tarih    : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
Makine   : $env:COMPUTERNAME
Kullanici: $env:USERNAME
Git Hash : $GitHash
Script   : deploy_local.ps1

Kopyalanan dosyalar: $CopiedCount
Atlatilan dosyalar : $SkippedCount
"@
    Set-Content -Path $ManifestPath -Value $Manifest -Encoding UTF8
    Write-INFO "Deploy manifest yazıldı: VCE_DEPLOY_MANIFEST.txt"
    Write-Host ""
}

# ── Docker Yeniden Başlat (opsiyonel) ────────────────────────────────────────

if ($RestartDocker -and -not $DryRun -and $CopiedCount -gt 0) {
    Write-Host "🐳 Docker yeniden başlatılıyor..." -ForegroundColor White

    $DockerComposePath = Split-Path $TargetDagsPath -Parent
    $ComposeFile = Join-Path $DockerComposePath "docker-compose.yml"

    if (Test-Path $ComposeFile) {
        try {
            Set-Location $DockerComposePath
            Write-INFO "docker-compose down..."
            docker-compose down --timeout 30 2>&1 | Out-Null
            Write-INFO "docker-compose up -d..."
            docker-compose up -d 2>&1 | Out-Null
            Write-OK "Docker yeniden başlatıldı."
        } catch {
            Write-FAIL "Docker yeniden başlatma hatası: $_"
        }
    } else {
        Write-WARN "docker-compose.yml bulunamadı: $ComposeFile"
    }
    Write-Host ""
}

# ── Özet ─────────────────────────────────────────────────────────────────────

Write-Host "════════════════════════════════════════════════" -ForegroundColor Blue
Write-Host "  Deploy Özeti" -ForegroundColor Blue
Write-Host "════════════════════════════════════════════════" -ForegroundColor Blue
Write-Host "  Kopyalanan : $CopiedCount dosya" -ForegroundColor $(if ($CopiedCount -gt 0) { "Green" } else { "White" })
Write-Host "  Atlatılan  : $SkippedCount dosya (değişmemiş)" -ForegroundColor White
Write-Host "  Hata       : $ErrorCount dosya" -ForegroundColor $(if ($ErrorCount -gt 0) { "Red" } else { "White" })
Write-Host ""

if ($ErrorCount -gt 0) {
    Write-FAIL "Deploy tamamlanamadı — $ErrorCount hata var."
    exit 1
} elseif ($CopiedCount -eq 0 -and -not $DryRun) {
    Write-INFO "Değişen dosya yok — deploy atlandı."
} else {
    if (-not $DryRun) {
        Write-OK "Deploy başarıyla tamamlandı!"
        Write-INFO "Airflow UI'da DAG'ları kontrol edin: http://localhost:8080"
        if (-not $RestartDocker) {
            Write-INFO "Docker yeniden başlatmak için: .\deploy_local.ps1 -RestartDocker"
        }
    } else {
        Write-OK "DRY RUN tamamlandı. Gerçek deploy için -DryRun parametresini kaldırın."
    }
}

Write-Host ""
