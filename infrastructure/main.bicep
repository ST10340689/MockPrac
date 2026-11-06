@description('Prefix for all AgriStream IoT resources')
param prefix string = 'agristream'

@description('Azure region for resources')
param location string = resourceGroup().location

@description('SQL Server administrator login')
param sqlAdminLogin string = 'cloudadmin'

@secure()
@description('SQL Server administrator password')
param sqlAdminPassword string

// ============================================================================
// 1. Azure SQL Server & Database (Users & DeviceRegistry Tables)
// ============================================================================
resource sqlServer 'Microsoft.Sql/servers@2022-05-01-preview' = {
  name: '${prefix}-sql-${uniqueString(resourceGroup().id)}'
  location: location
  properties: {
    administratorLogin: sqlAdminLogin
    administratorLoginPassword: sqlAdminPassword
    version: '12.0'
    minimalTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
  }
}

resource sqlFirewallAllowAzure 'Microsoft.Sql/servers/firewallRules@2022-05-01-preview' = {
  parent: sqlServer
  name: 'AllowAllWindowsAzureIps'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

resource sqlDatabase 'Microsoft.Sql/servers/databases@2022-05-01-preview' = {
  parent: sqlServer
  name: 'cldvpracdb'
  location: location
  sku: {
    name: 'GP_S_Gen5'
    tier: 'GeneralPurpose'
    family: 'Gen5'
    capacity: 2
  }
  properties: {
    collation: 'SQL_Latin1_General_CP1_CI_AS'
    maxSizeBytes: 34359738368
    autoPauseDelay: 60
    minCapacity: json('0.5')
  }
}

// ============================================================================
// 2. Azure Storage Account (Blob, Table, and Queue Storage)
// - Blob Container: "drone-images" (Drone aerial media)
// - Tables: "SensorTelemetry" & "DroneImageAudit" (High-frequency telemetry)
// - Queue: "irrigation-commands" (Irrigation Command queue)
// ============================================================================
resource storageAccount 'Microsoft.Storage/storageAccounts@2022-09-01' = {
  name: 'cldvprac${uniqueString(resourceGroup().id)}'
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2022-09-01' = {
  parent: storageAccount
  name: 'default'
}

resource droneImagesContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2022-09-01' = {
  parent: blobService
  name: 'drone-images'
  properties: {
    publicAccess: 'None'
  }
}

resource tableService 'Microsoft.Storage/storageAccounts/tableServices@2022-09-01' = {
  parent: storageAccount
  name: 'default'
}

resource sensorTelemetryTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2022-09-01' = {
  parent: tableService
  name: 'SensorTelemetry'
}

resource droneAuditTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2022-09-01' = {
  parent: tableService
  name: 'DroneImageAudit'
}

resource queueService 'Microsoft.Storage/storageAccounts/queueServices@2022-09-01' = {
  parent: storageAccount
  name: 'default'
}

resource irrigationCommandsQueue 'Microsoft.Storage/storageAccounts/queueServices/queues@2022-09-01' = {
  parent: queueService
  name: 'irrigation-commands'
}

// ============================================================================
// 3. Azure App Service Plan (Standard S1) & MVC Web Application
// with Auto-Scaling Rules based on CPU (> 70%) and Memory (> 75%) Thresholds
// ============================================================================
resource appServicePlan 'Microsoft.Web/serverfarms@2022-09-01' = {
  name: '${prefix}-asp-${uniqueString(resourceGroup().id)}'
  location: location
  sku: {
    name: 'S1'
    tier: 'Standard'
  }
  kind: 'linux'
  properties: {
    reserved: true
  }
}

resource mvcWebApp 'Microsoft.Web/sites@2022-09-01' = {
  name: '${prefix}-mvc-${uniqueString(resourceGroup().id)}'
  location: location
  kind: 'app,linux'
  properties: {
    serverFarmId: appServicePlan.id
    siteConfig: {
      linuxFxVersion: 'DOTNETCORE|8.0'
      alwaysOn: true
      healthCheckPath: '/api/health'
    }
  }
}

resource autoscaleSetting 'Microsoft.Insights/autoscalesettings@2022-10-01' = {
  name: '${prefix}-appservice-autoscale'
  location: location
  properties: {
    targetResourceUri: appServicePlan.id
    enabled: true
    profiles: [
      {
        name: 'AgriStreamDaylightPeakScaleProfile'
        capacity: {
          minimum: '1'
          maximum: '5'
          default: '1'
        }
        rules: [
          // Rule 1: Scale Out when Average CPU > 70% over 5 minutes
          {
            metricTrigger: {
              metricName: 'CpuPercentage'
              metricResourceUri: appServicePlan.id
              timeGrain: 'PT1M'
              statistic: 'Average'
              timeWindow: 'PT5M'
              timeAggregation: 'Average'
              operator: 'GreaterThan'
              threshold: 70
            }
            scaleAction: {
              direction: 'Increase'
              type: 'ChangeCount'
              value: '1'
              cooldown: 'PT5M'
            }
          }
          // Rule 2: Scale Out when Average Memory > 75% over 5 minutes
          {
            metricTrigger: {
              metricName: 'MemoryPercentage'
              metricResourceUri: appServicePlan.id
              timeGrain: 'PT1M'
              statistic: 'Average'
              timeWindow: 'PT5M'
              timeAggregation: 'Average'
              operator: 'GreaterThan'
              threshold: 75
            }
            scaleAction: {
              direction: 'Increase'
              type: 'ChangeCount'
              value: '1'
              cooldown: 'PT5M'
            }
          }
          // Rule 3: Scale In when Average CPU < 30% over 5 minutes (10m cooldown)
          {
            metricTrigger: {
              metricName: 'CpuPercentage'
              metricResourceUri: appServicePlan.id
              timeGrain: 'PT1M'
              statistic: 'Average'
              timeWindow: 'PT5M'
              timeAggregation: 'Average'
              operator: 'LessThan'
              threshold: 30
            }
            scaleAction: {
              direction: 'Decrease'
              type: 'ChangeCount'
              value: '1'
              cooldown: 'PT10M'
            }
          }
        ]
      }
    ]
  }
}

// ============================================================================
// 4. Serverless Data Pipeline — 4 Azure Functions on Dynamic Plan
// 1. DroneImageBlobTrigger (BlobTrigger on "drone-images/{name}")
// 2. GetLatestTelemetry (HttpTrigger GET on "telemetry")
// 3. TriggerIrrigationCommand (HttpTrigger POST on "irrigation/trigger")
// 4. ProcessIrrigationQueue (QueueTrigger on "irrigation-commands")
// ============================================================================
resource functionPlan 'Microsoft.Web/serverfarms@2022-09-01' = {
  name: '${prefix}-funcplan-${uniqueString(resourceGroup().id)}'
  location: location
  sku: {
    name: 'Y1'
    tier: 'Dynamic'
  }
  properties: {}
}

resource functionApp 'Microsoft.Web/sites@2022-09-01' = {
  name: '${prefix}-func-${uniqueString(resourceGroup().id)}'
  location: location
  kind: 'functionapp'
  properties: {
    serverFarmId: functionPlan.id
    siteConfig: {
      netFrameworkVersion: 'v8.0'
      appSettings: [
        {
          name: 'FUNCTIONS_EXTENSION_VERSION'
          value: '~4'
        }
        {
          name: 'FUNCTIONS_WORKER_RUNTIME'
          value: 'dotnet-isolated'
        }
      ]
    }
  }
}
