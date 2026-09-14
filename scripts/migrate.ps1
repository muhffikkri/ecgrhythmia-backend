[CmdletBinding()]
param(
    [string]$DatabaseUrl = $env:DATABASE_URL
)

$ErrorActionPreference = 'Stop'

if (-not $DatabaseUrl) {
    $envFile = Join-Path $PSScriptRoot '..\.env'
    if (Test-Path $envFile) {
        $line = Get-Content $envFile | Where-Object { $_ -match '^DATABASE_URL=' } | Select-Object -First 1
        if ($line) {
            $DatabaseUrl = ($line -replace '^DATABASE_URL=', '').Trim().Trim('"')
        }
    }
}

if (-not $DatabaseUrl -or $DatabaseUrl -match '\[PASSWORD') {
    throw 'DATABASE_URL belum dikonfigurasi. Set environment variable DATABASE_URL atau isi .env.'
}

if (-not (Get-Command psql -ErrorAction SilentlyContinue)) {
    throw 'psql tidak ditemukan. Instal PostgreSQL client terlebih dahulu.'
}

$migrations = Get-ChildItem (Join-Path $PSScriptRoot '..\migrations') -Filter '*.sql' | Sort-Object Name
if (-not $migrations) {
    throw 'Tidak ada file migrasi di folder migrations/.'
}

Write-Host 'Menjalankan migrasi PostgreSQL...'
foreach ($migration in $migrations) {
    Write-Host "  -> $($migration.Name)"
    & psql $DatabaseUrl --set ON_ERROR_STOP=1 --file $migration.FullName
    if ($LASTEXITCODE -ne 0) {
        throw "Migrasi $($migration.Name) gagal dengan exit code $LASTEXITCODE."
    }
}
Write-Host 'Migrasi PostgreSQL berhasil.'