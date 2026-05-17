# Quick Start — Social Listening EV Vietnam
# Run this script once to initialize the project.

Write-Host "=== Social Listening — EV Vietnam ===" -ForegroundColor Cyan

# 1. Copy env file if not exists
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "[1/4] .env created from .env.example — fill in your API keys!" -ForegroundColor Yellow
} else {
    Write-Host "[1/4] .env already exists" -ForegroundColor Green
}

# 2. Start databases and services
Write-Host "[2/4] Starting Docker services..." -ForegroundColor Cyan
docker compose up -d mongodb postgres
Start-Sleep -Seconds 5

# 3. Build and start Dagster + Streamlit
Write-Host "[3/4] Building and starting all services..." -ForegroundColor Cyan
docker compose up -d --build

# 4. Done
Write-Host ""
Write-Host "=== Services Running ===" -ForegroundColor Green
Write-Host "  Dagster UI   : http://localhost:3000" -ForegroundColor White
Write-Host "  Dashboard    : http://localhost:8501" -ForegroundColor White
Write-Host "  MongoDB      : localhost:27017" -ForegroundColor White
Write-Host "  PostgreSQL   : localhost:5432" -ForegroundColor White
Write-Host ""
Write-Host "Next: Open .env and add your API keys, then trigger a run in Dagster UI." -ForegroundColor Yellow
