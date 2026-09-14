[CmdletBinding()]
param(
    [switch]$SkipMigration
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
    if (-not $SkipMigration) {
        & (Join-Path $PSScriptRoot 'migrate.ps1')
    }

    $env:OPENSSL_DIR = Join-Path $root 'openssl-custom'
    $env:OPENSSL_STATIC = '1'

    cargo fmt --all -- --check
    if ($LASTEXITCODE -ne 0) {
        Write-Warning 'cargo fmt menemukan file yang belum terformat; test tetap dilanjutkan.'
    }

    cargo test --all-targets
    if ($LASTEXITCODE -ne 0) { throw 'Unit/integration test gagal.' }

    Write-Host 'Unit dan integration test berhasil.'
    Write-Host 'E2E smoke test memerlukan server aktif, PostgreSQL, MQTT, dan test client; belum ada suite E2E otomatis di repo ini.'
} finally {
    Pop-Location
}