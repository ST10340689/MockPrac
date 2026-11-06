<#
.SYNOPSIS
    AgriStream IoT — Automated Azure Provisioning & Autoscaling Setup Script.
    Provisions:
      - Azure App Service hosting the ASP.NET Core MVC Web Application
      - Azure SQL Database (cldvpracdb) managing Users (Farmer, Agronomist, Admin) & DeviceRegistry
      - Azure Storage (cldvpracstorage) with Blob Storage (drone-images), Table Storage (SensorTelemetry), and Queue Storage (irrigation-commands)
      - Serverless Data Pipeline with 4 Azure Functions (BlobTrigger, HttpTrigger GET, HttpTrigger POST, QueueTrigger)
      - Auto-Scaling Rules (Scale-out) based on CPU (> 70%) and Memory (> 75%) utilization thresholds
#>

param(
    [string]$ResourceGroupName = "rg-agristream-iot-prod",
    [string]$Location = "southafricanorth",
    [string]$SqlServerName = "cldvprac",
    [string]$SqlDatabaseName = "cldvpracdb",
    [string]$StorageAccountName = "cldvpracstorage",
    [string]$AppServicePlanName = "asp-agristream-mvc-s1",
    [string]$MvcWebAppName = "app-agristream-iot-mvc",
    [string]$FunctionAppName = "func-agristream-pipeline",
    [string]$SqlAdminUser = "cloudadmin",
    [string]$SqlAdminPassword = "P@ssw0rd12345!"
)

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host " AGRISTREAM IoT — AZURE REAL-TIME PROCESSING PROVISIONING SCRIPT" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

# 1. Create Resource Group
Write-Host "`n[1/6] Creating Resource Group: $ResourceGroupName..." -ForegroundColor Yellow
az group create --name $ResourceGroupName --location $Location

# 2. Provision Azure App Service Plan (Standard S1) & MVC Web App
Write-Host "`n[2/6] Provisioning Azure App Service for MVC Web Application..." -ForegroundColor Yellow
az appservice plan create `
    --name $AppServicePlanName `
    --resource-group $ResourceGroupName `
    --location $Location `
    --sku S1 `
    --is-linux

az webapp create `
    --name $MvcWebAppName `
    --resource-group $ResourceGroupName `
    --plan $AppServicePlanName `
    --runtime "DOTNETCORE:8.0"

# 3. Provision Azure SQL Database (Users & DeviceRegistry Tables)
Write-Host "`n[3/6] Provisioning Azure SQL Server ($SqlServerName) & Database ($SqlDatabaseName)..." -ForegroundColor Yellow
az sql server create `
    --name $SqlServerName `
    --resource-group $ResourceGroupName `
    --location $Location `
    --admin-user $SqlAdminUser `
    --admin-password $SqlAdminPassword

az sql server firewall-rule create `
    --resource-group $ResourceGroupName `
    --server $SqlServerName `
    --name "AllowAzureServices" `
    --start-ip-address 0.0.0.0 `
    --end-ip-address 0.0.0.0

az sql db create `
    --resource-group $ResourceGroupName `
    --server $SqlServerName `
    --name $SqlDatabaseName `
    --edition GeneralPurpose `
    --compute-model Serverless `
    --family Gen5 `
    --capacity 2 `
    --auto-pause-delay 60

# 4. Provision Azure Storage (Blob: drone-images, Table: SensorTelemetry, Queue: irrigation-commands)
Write-Host "`n[4/6] Provisioning Azure Storage ($StorageAccountName: Blob, Table, Queue)..." -ForegroundColor Yellow
az storage account create `
    --name $StorageAccountName `
    --resource-group $ResourceGroupName `
    --location $Location `
    --sku Standard_LRS `
    --kind StorageV2

az storage container create --name "drone-images" --account-name $StorageAccountName
az storage table create --name "SensorTelemetry" --account-name $StorageAccountName
az storage table create --name "DroneImageAudit" --account-name $StorageAccountName
az storage queue create --name "irrigation-commands" --account-name $StorageAccountName

# 5. Provision Azure Functions App for the 4 Serverless Pipeline Functions
Write-Host "`n[5/6] Provisioning Azure Function App ($FunctionAppName)..." -ForegroundColor Yellow
az functionapp create `
    --name $FunctionAppName `
    --resource-group $ResourceGroupName `
    --storage-account $StorageAccountName `
    --consumption-plan-location $Location `
    --runtime dotnet-isolated `
    --runtime-version 8.0 `
    --functions-version 4 `
    --os-type Linux

# 6. Configure Auto-Scaling Rules (Scale-Out) based on CPU & Memory Thresholds
Write-Host "`n[6/6] Configuring App Service Auto-Scaling Rules (CPU > 70%, Memory > 75%)..." -ForegroundColor Yellow
az monitor autoscale create `
    --resource-group $ResourceGroupName `
    --name "autoscale-$MvcWebAppName" `
    --resource $AppServicePlanName `
    --resource-type Microsoft.Web/serverfarms `
    --min-count 1 `
    --max-count 5 `
    --count 1

# Scale Out Rule 1: High CPU > 70% over 5 minutes -> Add 1 instance
az monitor autoscale rule create `
    --resource-group $ResourceGroupName `
    --autoscale-name "autoscale-$MvcWebAppName" `
    --condition "Percentage CPU > 70 avg 5m" `
    --scale out 1 `
    --cooldown 5

# Scale Out Rule 2: High Memory > 75% over 5 minutes -> Add 1 instance
az monitor autoscale rule create `
    --resource-group $ResourceGroupName `
    --autoscale-name "autoscale-$MvcWebAppName" `
    --condition "Memory Percentage > 75 avg 5m" `
    --scale out 1 `
    --cooldown 5

# Scale In Rule: Low CPU < 30% over 5 minutes -> Remove 1 instance
az monitor autoscale rule create `
    --resource-group $ResourceGroupName `
    --autoscale-name "autoscale-$MvcWebAppName" `
    --condition "Percentage CPU < 30 avg 5m" `
    --scale in 1 `
    --cooldown 10

Write-Host "`n======================================================================" -ForegroundColor Green
Write-Host " AGRISTREAM IoT INFRASTRUCTURE & AUTOSCALE POLICIES DEPLOYED!" -ForegroundColor Green
Write-Host " MVC App Service: $MvcWebAppName" -ForegroundColor Green
Write-Host " SQL Database:    $SqlDatabaseName on $SqlServerName.database.windows.net" -ForegroundColor Green
Write-Host " Cloud Storage:   $StorageAccountName (Blob: drone-images, Table: SensorTelemetry, Queue: irrigation-commands)" -ForegroundColor Green
Write-Host " Function App:    $FunctionAppName (4 Functions: Blob, HTTP GET, HTTP POST, Queue)" -ForegroundColor Green
Write-Host "======================================================================" -ForegroundColor Green
