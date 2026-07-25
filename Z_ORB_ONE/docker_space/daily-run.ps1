# daily-run.ps1
# 用途：
# 1. 上傳今日 stock_data.py 至 S3
# 2. 確認上傳成功
# 3. 啟動 ECS Fargate Task

$ErrorActionPreference = "Stop"

#==============================
# AWS
#==============================
$Bucket = "leegueishen-quant-trading-17"
$S3Folder = "exchange"

$Cluster = "quant-cluster"
$TaskDefinition = "quant-trading-task"

$Subnet = "subnet-109d7e58"
$SecurityGroup = "sg-0859a0e3c6b601ff5"

#==============================
# Local File
#==============================
$StockFile = ".\external\stock_data.py"

if (-not (Test-Path $StockFile)) {

    throw "找不到 $StockFile"

}

#==============================
# Upload stock_data.py
#==============================

Write-Host ""
Write-Host "===== Upload stock_data.py ====="

aws s3 cp `
    $StockFile `
    s3://$Bucket/$S3Folder/stock_data.py

if ($LASTEXITCODE -ne 0) {

    throw "Upload stock_data.py failed."

}

#==============================
# Verify
#==============================

Write-Host ""
Write-Host "===== Verify Upload ====="

aws s3 ls `
    s3://$Bucket/$S3Folder/stock_data.py

if ($LASTEXITCODE -ne 0) {

    throw "Verify stock_data.py failed."

}

#==============================
# Run ECS Task
#==============================

Write-Host ""
Write-Host "===== Run ECS Task ====="

aws ecs run-task `
    --cluster $Cluster `
    --launch-type FARGATE `
    --task-definition $TaskDefinition `
    --network-configuration "awsvpcConfiguration={subnets=[$Subnet],securityGroups=[$SecurityGroup],assignPublicIp=ENABLED}" `
    --no-cli-pager

if ($LASTEXITCODE -ne 0) {

    throw "Run ECS Task failed."

}

#==============================
# Finish
#==============================

Write-Host ""
Write-Host "========================================"
Write-Host "Daily Run Completed"
Write-Host "========================================"
Write-Host ""
Write-Host "若需即時監控 Log："
Write-Host ""
Write-Host "aws logs tail /ecs/quant-trading --since 30m --follow"
Write-Host ""