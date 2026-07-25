$Cluster = "quant-cluster"

$TaskArn = aws ecs list-tasks `
    --cluster $Cluster `
    --desired-status RUNNING `
    --query "taskArns[0]" `
    --output text

if ($TaskArn -eq "None") {
    Write-Host "目前沒有執行中的 Task"
    exit
}

Write-Host "Stopping:"
Write-Host $TaskArn

aws ecs stop-task `
    --cluster $Cluster `
    --task $TaskArn `
    --no-cli-pager

Write-Host "Stop request sent."