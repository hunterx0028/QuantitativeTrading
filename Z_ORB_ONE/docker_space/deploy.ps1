# deploy.ps1
# 用途：更新 Dockerfile CMD、重新 build image、push 到 AWS ECR
# 執行位置：C:\Users\leegu\quant_trading

$ErrorActionPreference = "Stop"

# ==============================
# 每次策略檔名有變更，只改這一行
# ==============================
$PythonFile = "execute_strategy_broken_high_falling_docker_v17.py"

# ==============================
# AWS / Docker 設定
# ==============================
$Region = "ap-northeast-1"
$AccountId = "070747961467"
$RepositoryName = "quant-trading"

$ImageName = "quant-trading"
$ImageTag = "latest"

$EcrRegistry = "$AccountId.dkr.ecr.$Region.amazonaws.com"
$EcrImage = "$EcrRegistry/$RepositoryName`:$ImageTag"

# ==============================
# 基本檢查
# ==============================
if (-not (Test-Path ".\Dockerfile")) {
    throw "找不到 Dockerfile，請確認目前目錄是否為 quant_trading。"
}

if (-not (Test-Path ".\$PythonFile")) {
    throw "找不到 Python 檔案：$PythonFile"
}

# ==============================
# 更新 Dockerfile CMD
# ==============================
Write-Host "===== Update Dockerfile CMD ====="

(Get-Content ".\Dockerfile") |
ForEach-Object {
    if ($_ -match '^CMD\s+') {
        'CMD ["python", "' + $PythonFile + '"]'
    }
    else {
        $_
    }
} | Set-Content ".\Dockerfile"

Write-Host "Python file: $PythonFile"

# ==============================
# Docker Build
# ==============================
Write-Host "===== Docker build ====="

docker build --no-cache -t $ImageName .

if ($LASTEXITCODE -ne 0) {
    throw "Docker build failed."
}

# ==============================
# Login ECR
# ==============================
Write-Host "===== Login to ECR ====="

aws ecr get-login-password --region $Region `
| docker login --username AWS --password-stdin $EcrRegistry

if ($LASTEXITCODE -ne 0) {
    throw "Docker login failed."
}

# ==============================
# Docker Tag
# ==============================
Write-Host "===== Docker tag ====="

docker tag "$ImageName`:latest" $EcrImage

if ($LASTEXITCODE -ne 0) {
    throw "Docker tag failed."
}

# ==============================
# Docker Push
# ==============================
Write-Host "===== Docker push ====="

docker push $EcrImage

if ($LASTEXITCODE -ne 0) {
    throw "Docker push failed."
}

# ==============================
# 完成
# ==============================
Write-Host ""
Write-Host "========================================"
Write-Host "Deploy completed"
Write-Host "Image      : $EcrImage"
Write-Host "Python file: $PythonFile"
Write-Host "========================================"