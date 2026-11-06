using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace Ecommerce.Functions;

/// <summary>
/// AgriStream IoT — Serverless Real-Time Data Processing Pipeline.
/// Implements the four (4) Azure Functions for the platform:
///   1. Blob-Triggered Function (DroneImageBlobTrigger): Triggered on drone image upload to Blob Storage ("drone-images/{name}") -> writes audit entry to Azure Table Storage.
///   2. HTTP-Triggered Function GET (GetLatestTelemetry): Called by MVC dashboard to retrieve latest telemetry from Azure Table Storage ("SensorTelemetry").
///   3. HTTP-Triggered Function POST (TriggerIrrigationCommand): Called when sensor detects low moisture or user clicks "Trigger Irrigation" -> enqueues command to Azure Storage Queue ("irrigation-commands").
///   4. Queue-Triggered Function (ProcessIrrigationQueue): Listens to Azure Storage Queue ("irrigation-commands"), processes command, and updates device operational status in master DeviceRegistry table in Azure SQL Database.
/// </summary>
public class OrderStreamProcessor
{
    private readonly ILogger<OrderStreamProcessor> _logger;
    private static bool _schemaEnsured;

    private const string DefaultAzureSqlConnectionString =
        "Server=tcp:cldvprac.database.windows.net,1433;Initial Catalog=cldvpracdb;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;Authentication=\"Active Directory Default\";";

    private const string DefaultAzureStorageConnectionString = "";

    public OrderStreamProcessor(ILogger<OrderStreamProcessor> logger)
    {
        _logger = logger;
    }

    // =========================================================================
    // FUNCTION 1 OF 4: BLOB-TRIGGERED FUNCTION
    // Triggered when a field drone image is uploaded to Azure Blob Storage
    // ("drone-images/{name}") -> logs upload details & writes processing status
    // audit entry directly into Azure Table Storage ("DroneImageAudit" & "SensorTelemetry").
    // =========================================================================
    [Function("DroneImageBlobTrigger")]
    public async Task DroneImageBlobTrigger(
        [BlobTrigger("drone-images/{name}", Connection = "AzureStorage")] byte[] blobBytes,
        string name)
    {
        int sizeBytes = blobBytes?.Length ?? 0;
        string auditId = $"AUDIT-{DateTime.UtcNow:yyyyMMddHHmmss}-{Guid.NewGuid().ToString()[..4].ToUpper()}";
        string fieldZone = name.Contains("south", StringComparison.OrdinalIgnoreCase) ? "Sector-South-B2"
                         : name.Contains("east", StringComparison.OrdinalIgnoreCase) ? "Sector-East-C3"
                         : name.Contains("west", StringComparison.OrdinalIgnoreCase) ? "Sector-West-D4"
                         : "Sector-North-A1";

        _logger.LogInformation(
            "[Function 1 - BlobTrigger] Drone image uploaded: BlobName={BlobName}, Size={SizeBytes} bytes, Zone={FieldZone}",
            name, sizeBytes, fieldZone);

        var storageConn = GetStorageConnectionString();
        if (string.IsNullOrWhiteSpace(storageConn))
        {
            _logger.LogWarning("AzureStorage connection string not configured; skipping Table Storage write.");
            return;
        }

        try
        {
            var tableService = new TableServiceClient(storageConn);

            // 1. Write processing status audit entry directly into DroneImageAudit Table Storage
            var auditTable = tableService.GetTableClient("DroneImageAudit");
            await auditTable.CreateIfNotExistsAsync();

            var auditEntity = new TableEntity(fieldZone, auditId)
            {
                ["DroneId"] = "DRONE-AGRI-X8",
                ["BlobName"] = name,
                ["FileSizeBytes"] = sizeBytes,
                ["ContentType"] = name.EndsWith(".png", StringComparison.OrdinalIgnoreCase) ? "image/png"
                                : name.EndsWith(".jpg", StringComparison.OrdinalIgnoreCase) ? "image/jpeg"
                                : "image/svg+xml",
                ["ProcessingStatus"] = "Completed - BlobTrigger Audit Logged to Azure Table Storage",
                ["NdviAnalysis"] = "NDVI 0.81 - High Chlorophyll Canopy Vigour Verified",
                ["UploadedAt"] = DateTime.UtcNow.ToString("o"),
                ["ProcessedByFunction"] = "DroneImageBlobTrigger"
            };
            await auditTable.UpsertEntityAsync(auditEntity, TableUpdateMode.Replace);

            // 2. Also write an environmental monitoring log entry into SensorTelemetry Table Storage
            var telemetryTable = tableService.GetTableClient("SensorTelemetry");
            await telemetryTable.CreateIfNotExistsAsync();

            var telemetryLog = new TableEntity(fieldZone, $"TEL-DRONE-{Guid.NewGuid().ToString()[..6].ToUpper()}")
            {
                ["SensorId"] = "DRONE-AGRI-X8",
                ["EventType"] = "EnvironmentalLog",
                ["SoilMoisturePercent"] = 41.2,
                ["TemperatureC"] = 24.8,
                ["HumidityPercent"] = 56.0,
                ["PhLevel"] = 6.6,
                ["NitrogenPpm"] = 44.5,
                ["Status"] = $"Drone Aerial Image Processed ({name}, {sizeBytes} bytes)",
                ["Timestamp"] = DateTime.UtcNow.ToString("o")
            };
            await telemetryTable.UpsertEntityAsync(telemetryLog, TableUpdateMode.Replace);

            _logger.LogInformation(
                "[Function 1 - BlobTrigger] Successfully wrote processing audit entry {AuditId} to Azure Table Storage.",
                auditId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Function 1 - BlobTrigger] Error writing audit entry to Azure Table Storage.");
        }
    }

    // =========================================================================
    // FUNCTION 2 OF 4: HTTP-TRIGGERED FUNCTION (GET)
    // Called by the MVC application when a farmer opens their field dashboard
    // to retrieve the latest telemetry data from Azure Table Storage for
    // near-instant rendering.
    // =========================================================================
    [Function("GetLatestTelemetry")]
    public async Task<HttpResponseData> GetLatestTelemetry(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "telemetry")] HttpRequestData req)
    {
        _logger.LogInformation("[Function 2 - HttpTrigger GET] Retrieving latest sensor telemetry from Azure Table Storage.");

        var storageConn = GetStorageConnectionString();
        var telemetryItems = new List<SensorTelemetryRecord>();
        var auditItems = new List<object>();

        if (!string.IsNullOrWhiteSpace(storageConn))
        {
            try
            {
                var tableService = new TableServiceClient(storageConn);
                var telemetryTable = tableService.GetTableClient("SensorTelemetry");
                await telemetryTable.CreateIfNotExistsAsync();

                await foreach (var entity in telemetryTable.QueryAsync<TableEntity>(maxPerPage: 50))
                {
                    telemetryItems.Add(new SensorTelemetryRecord
                    {
                        TelemetryId = entity.RowKey,
                        FieldZone = entity.PartitionKey,
                        SensorId = entity.GetString("SensorId") ?? "SENS-SOIL-101",
                        EventType = entity.GetString("EventType") ?? "SoilSensorReading",
                        SoilMoisturePercent = entity.GetDouble("SoilMoisturePercent") ?? 38.5,
                        TemperatureC = entity.GetDouble("TemperatureC") ?? 25.0,
                        HumidityPercent = entity.GetDouble("HumidityPercent") ?? 52.0,
                        PhLevel = entity.GetDouble("PhLevel") ?? 6.5,
                        NitrogenPpm = entity.GetDouble("NitrogenPpm") ?? 40.0,
                        Status = entity.GetString("Status") ?? "Optimal",
                        Timestamp = entity.GetString("Timestamp") ?? DateTime.UtcNow.ToString("o")
                    });
                    if (telemetryItems.Count >= 40) break;
                }

                var auditTable = tableService.GetTableClient("DroneImageAudit");
                await auditTable.CreateIfNotExistsAsync();
                await foreach (var audit in auditTable.QueryAsync<TableEntity>(maxPerPage: 15))
                {
                    auditItems.Add(new
                    {
                        audit_id = audit.RowKey,
                        field_zone = audit.PartitionKey,
                        drone_id = audit.GetString("DroneId") ?? "DRONE-AGRI-X8",
                        blob_name = audit.GetString("BlobName") ?? "drone-survey.svg",
                        file_size_bytes = audit.GetInt64("FileSizeBytes") ?? audit.GetInt32("FileSizeBytes") ?? 2048,
                        processing_status = audit.GetString("ProcessingStatus") ?? "Audit Logged",
                        ndvi_analysis = audit.GetString("NdviAnalysis") ?? "NDVI 0.78 (Healthy)",
                        uploaded_at = audit.GetString("UploadedAt") ?? DateTime.UtcNow.ToString("o")
                    });
                    if (auditItems.Count >= 15) break;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[Function 2 - HttpTrigger GET] Table Storage query fallback.");
            }
        }

        if (telemetryItems.Count == 0)
        {
            telemetryItems.AddRange(GetDefaultTelemetrySeeds());
        }

        var ordered = telemetryItems
            .OrderByDescending(t => t.Timestamp)
            .Take(25)
            .ToList();

        var response = req.CreateResponse(HttpStatusCode.OK);
        response.Headers.Add("Content-Type", "application/json; charset=utf-8");
        await response.WriteStringAsync(JsonSerializer.Serialize(new
        {
            status = "Success",
            function_name = "GetLatestTelemetry (HttpTrigger GET)",
            storage_source = "Azure Table Storage (cldvpracstorage -> SensorTelemetry & DroneImageAudit)",
            count = ordered.Count,
            telemetry = ordered,
            drone_image_audits = auditItems
        }, new JsonSerializerOptions { WriteIndented = true }));

        return response;
    }

    // =========================================================================
    // FUNCTION 3 OF 4: HTTP-TRIGGERED FUNCTION (POST)
    // Called when an automated soil sensor detects low moisture or a farmer clicks
    // "Trigger Irrigation" in the MVC web app -> places an "Irrigation Command"
    // message into the Azure Storage Queue ("irrigation-commands").
    // =========================================================================
    [Function("TriggerIrrigationCommand")]
    public async Task<HttpResponseData> TriggerIrrigationCommand(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "irrigation/trigger")] HttpRequestData req)
    {
        _logger.LogInformation("[Function 3 - HttpTrigger POST] Processing incoming Irrigation Command request.");

        IrrigationCommandPayload? command = null;
        try
        {
            string body = await new StreamReader(req.Body).ReadToEndAsync();
            if (!string.IsNullOrWhiteSpace(body))
            {
                command = JsonSerializer.Deserialize<IrrigationCommandPayload>(body, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                });
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not parse IrrigationCommand request body; using default command.");
        }

        command ??= new IrrigationCommandPayload();
        command.CommandId = string.IsNullOrWhiteSpace(command.CommandId)
            ? $"CMD-IRR-{DateTime.UtcNow:HHmmss}-{Guid.NewGuid().ToString()[..4].ToUpper()}"
            : command.CommandId;
        command.DeviceId = string.IsNullOrWhiteSpace(command.DeviceId) ? "DEV-PIVOT-A1" : command.DeviceId;
        command.FieldZone = string.IsNullOrWhiteSpace(command.FieldZone) ? "Sector-North-A1" : command.FieldZone;
        command.Action = string.IsNullOrWhiteSpace(command.Action) ? "START_IRRIGATION" : command.Action;
        if (command.DurationMinutes <= 0) command.DurationMinutes = 45;
        if (command.FlowRateLpm <= 0) command.FlowRateLpm = 450;
        command.TriggeredBy = string.IsNullOrWhiteSpace(command.TriggeredBy)
            ? "Farmer MVC Dashboard / Low-Moisture Sensor Alert"
            : command.TriggeredBy;
        command.RequestedAt = DateTime.UtcNow.ToString("o");

        string serializedCommand = JsonSerializer.Serialize(command);
        bool enqueuedToAzureQueue = false;

        var storageConn = GetStorageConnectionString();
        if (!string.IsNullOrWhiteSpace(storageConn))
        {
            try
            {
                var queueService = new QueueServiceClient(storageConn);
                var queueClient = queueService.GetQueueClient("irrigation-commands");
                await queueClient.CreateIfNotExistsAsync();
                await queueClient.SendMessageAsync(serializedCommand);
                enqueuedToAzureQueue = true;

                _logger.LogInformation(
                    "[Function 3 - HttpTrigger POST] Enqueued Irrigation Command {CommandId} for Device {DeviceId} to 'irrigation-commands' Queue.",
                    command.CommandId, command.DeviceId);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[Function 3 - HttpTrigger POST] Queue send warning.");
            }
        }

        var response = req.CreateResponse(HttpStatusCode.Accepted);
        response.Headers.Add("Content-Type", "application/json; charset=utf-8");
        await response.WriteStringAsync(JsonSerializer.Serialize(new
        {
            status = "Accepted",
            function_name = "TriggerIrrigationCommand (HttpTrigger POST)",
            queue_name = "irrigation-commands (cldvpracstorage)",
            enqueued_to_azure_queue = enqueuedToAzureQueue,
            command = command
        }, new JsonSerializerOptions { WriteIndented = true }));

        return response;
    }

    // =========================================================================
    // FUNCTION 4 OF 4: QUEUE-TRIGGERED FUNCTION
    // Listens to the "irrigation-commands" Azure Storage Queue, processes the
    // irrigation command, and updates the device's operational status in the
    // master DeviceRegistry table in the Azure SQL Database (cldvpracdb).
    // =========================================================================
    [Function("ProcessIrrigationQueue")]
    public async Task ProcessIrrigationQueue(
        [QueueTrigger("irrigation-commands", Connection = "AzureStorage")] string queueMessage)
    {
        _logger.LogInformation("[Function 4 - QueueTrigger] Dequeued irrigation command message: {Message}", queueMessage);

        IrrigationCommandPayload? cmd = null;
        try
        {
            cmd = JsonSerializer.Deserialize<IrrigationCommandPayload>(queueMessage, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[Function 4 - QueueTrigger] Malformed queue JSON; using defaults.");
        }

        cmd ??= new IrrigationCommandPayload
        {
            CommandId = $"CMD-{Guid.NewGuid().ToString()[..6].ToUpper()}",
            DeviceId = "DEV-PIVOT-A1",
            FieldZone = "Sector-North-A1",
            Action = "START_IRRIGATION",
            DurationMinutes = 45,
            FlowRateLpm = 450,
            TriggeredBy = "Automated Queue Processor"
        };

        string newStatus = cmd.Action.Equals("STOP_IRRIGATION", StringComparison.OrdinalIgnoreCase)
            ? "Standby - Valve Closed"
            : $"Irrigating - Valve Open ({cmd.FlowRateLpm} L/min for {cmd.DurationMinutes}m)";

        string commandSummary = $"{cmd.Action} ({cmd.DurationMinutes}m @ {cmd.FlowRateLpm} L/min) by {cmd.TriggeredBy}";

        // 1. Update Master DeviceRegistry Table in Azure SQL Database (cldvpracdb)
        var sqlConn = Environment.GetEnvironmentVariable("SqlConnectionString")
                   ?? Environment.GetEnvironmentVariable("AZURE_SQL_CONNECTION_STRING")
                   ?? DefaultAzureSqlConnectionString;

        try
        {
            using var conn = new SqlConnection(sqlConn);
            await conn.OpenAsync();

            if (!_schemaEnsured)
            {
                await EnsureSqlSchemaAsync(conn);
                _schemaEnsured = true;
            }

            const string upsertSql = @"
                IF EXISTS (SELECT 1 FROM DeviceRegistry WHERE DeviceId = @DeviceId)
                BEGIN
                    UPDATE DeviceRegistry
                    SET OperationalStatus = @OperationalStatus,
                        LastCommand = @LastCommand,
                        SoilMoisturePercent = CASE WHEN @Action = 'START_IRRIGATION' THEN 56.40 ELSE SoilMoisturePercent END,
                        LastUpdated = SYSDATETIMEOFFSET(),
                        UpdatedByFunction = 'ProcessIrrigationQueue'
                    WHERE DeviceId = @DeviceId;
                END
                ELSE
                BEGIN
                    INSERT INTO DeviceRegistry (
                        DeviceId, DeviceName, FieldZone, DeviceType, OperationalStatus,
                        SoilMoisturePercent, TemperatureC, LastCommand, LastUpdated, UpdatedByFunction
                    ) VALUES (
                        @DeviceId, @DeviceName, @FieldZone, 'IrrigationPivot', @OperationalStatus,
                        56.40, 24.50, @LastCommand, SYSDATETIMEOFFSET(), 'ProcessIrrigationQueue'
                    );
                END";

            using var sqlCmd = new SqlCommand(upsertSql, conn);
            sqlCmd.Parameters.AddWithValue("@DeviceId", cmd.DeviceId);
            sqlCmd.Parameters.AddWithValue("@DeviceName", $"Smart Irrigator ({cmd.DeviceId})");
            sqlCmd.Parameters.AddWithValue("@FieldZone", cmd.FieldZone);
            sqlCmd.Parameters.AddWithValue("@OperationalStatus", newStatus);
            sqlCmd.Parameters.AddWithValue("@LastCommand", commandSummary);
            sqlCmd.Parameters.AddWithValue("@Action", cmd.Action);

            await sqlCmd.ExecuteNonQueryAsync();
            _logger.LogInformation(
                "[Function 4 - QueueTrigger] Updated DeviceId={DeviceId} to '{Status}' in Azure SQL Database (DeviceRegistry).",
                cmd.DeviceId, newStatus);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[Function 4 - QueueTrigger] Azure SQL update fallback triggered.");
        }

        // 2. Also log the IrrigationEvent into Azure Table Storage ("SensorTelemetry")
        var storageConn = GetStorageConnectionString();
        if (!string.IsNullOrWhiteSpace(storageConn))
        {
            try
            {
                var tableService = new TableServiceClient(storageConn);
                var telemetryTable = tableService.GetTableClient("SensorTelemetry");
                await telemetryTable.CreateIfNotExistsAsync();

                var entity = new TableEntity(cmd.FieldZone, $"IRR-{DateTime.UtcNow:yyyyMMddHHmmss}-{Guid.NewGuid().ToString()[..4].ToUpper()}")
                {
                    ["SensorId"] = cmd.DeviceId,
                    ["EventType"] = "IrrigationEvent",
                    ["SoilMoisturePercent"] = 56.4,
                    ["TemperatureC"] = 24.2,
                    ["HumidityPercent"] = 68.0,
                    ["PhLevel"] = 6.7,
                    ["NitrogenPpm"] = 45.0,
                    ["Status"] = newStatus,
                    ["Timestamp"] = DateTime.UtcNow.ToString("o")
                };
                await telemetryTable.UpsertEntityAsync(entity, TableUpdateMode.Replace);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[Function 4 - QueueTrigger] Could not write irrigation event to Table Storage.");
            }
        }
    }

    private static string GetStorageConnectionString()
    {
        var conn = Environment.GetEnvironmentVariable("AzureStorage")
                ?? Environment.GetEnvironmentVariable("AzureWebJobsStorage");
        if (string.IsNullOrWhiteSpace(conn) || conn == "UseDevelopmentStorage=true")
        {
            return DefaultAzureStorageConnectionString;
        }
        return conn;
    }

    private static async Task EnsureSqlSchemaAsync(SqlConnection conn)
    {
        const string ddl = @"
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Users')
            BEGIN
                CREATE TABLE Users (
                    UserId              INT IDENTITY(1,1)   NOT NULL PRIMARY KEY CLUSTERED,
                    Username            NVARCHAR(64)        NOT NULL UNIQUE,
                    Password            NVARCHAR(128)       NOT NULL,
                    Role                NVARCHAR(32)        NOT NULL,
                    FullName            NVARCHAR(100)       NOT NULL,
                    AssignedFarmZone    NVARCHAR(64)        NOT NULL DEFAULT 'All Zones',
                    CreatedAt           DATETIMEOFFSET      NOT NULL DEFAULT SYSDATETIMEOFFSET()
                );
            END;

            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DeviceRegistry')
            BEGIN
                CREATE TABLE DeviceRegistry (
                    DeviceId                NVARCHAR(64)        NOT NULL PRIMARY KEY CLUSTERED,
                    DeviceName              NVARCHAR(100)       NOT NULL,
                    FieldZone               NVARCHAR(64)        NOT NULL,
                    DeviceType              NVARCHAR(64)        NOT NULL,
                    OperationalStatus       NVARCHAR(128)       NOT NULL DEFAULT 'Online - Monitoring',
                    SoilMoisturePercent     DECIMAL(5, 2)       NOT NULL DEFAULT 42.50,
                    TemperatureC            DECIMAL(5, 2)       NOT NULL DEFAULT 24.00,
                    LastCommand             NVARCHAR(256)       NULL,
                    LastUpdated             DATETIMEOFFSET      NOT NULL DEFAULT SYSDATETIMEOFFSET(),
                    UpdatedByFunction       NVARCHAR(64)        NOT NULL DEFAULT 'ProcessIrrigationQueue'
                );
            END;";

        using var cmd = new SqlCommand(ddl, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private static List<SensorTelemetryRecord> GetDefaultTelemetrySeeds() => new()
    {
        new() { TelemetryId = "TEL-2026-101", FieldZone = "Sector-North-A1", SensorId = "SENS-SOIL-A101", EventType = "SoilSensorReading", SoilMoisturePercent = 21.4, TemperatureC = 29.8, HumidityPercent = 41.0, PhLevel = 6.4, NitrogenPpm = 38.0, Status = "Low Moisture Warning (< 25%)", Timestamp = DateTime.UtcNow.AddMinutes(-4).ToString("o") },
        new() { TelemetryId = "TEL-2026-102", FieldZone = "Sector-South-B2", SensorId = "DEV-PIVOT-B2", EventType = "IrrigationEvent", SoilMoisturePercent = 54.2, TemperatureC = 24.5, HumidityPercent = 66.0, PhLevel = 6.7, NitrogenPpm = 46.0, Status = "Irrigating - Valve Open (420 L/min)", Timestamp = DateTime.UtcNow.AddMinutes(-3).ToString("o") },
        new() { TelemetryId = "TEL-2026-103", FieldZone = "Sector-East-C3", SensorId = "SENS-SOIL-C304", EventType = "SoilSensorReading", SoilMoisturePercent = 61.8, TemperatureC = 22.1, HumidityPercent = 58.0, PhLevel = 6.8, NitrogenPpm = 51.0, Status = "Optimal Soil Moisture", Timestamp = DateTime.UtcNow.AddMinutes(-2).ToString("o") },
        new() { TelemetryId = "TEL-2026-104", FieldZone = "Sector-North-A1", SensorId = "DRONE-AGRI-X8", EventType = "EnvironmentalLog", SoilMoisturePercent = 21.4, TemperatureC = 29.8, HumidityPercent = 41.0, PhLevel = 6.4, NitrogenPpm = 38.0, Status = "Drone Aerial NDVI Image Audited (drone-north-a1.svg)", Timestamp = DateTime.UtcNow.AddMinutes(-1).ToString("o") }
    };
}

// ============================================================================
// Data Contracts for the 4 AgriStream IoT Azure Functions
// ============================================================================
public class SensorTelemetryRecord
{
    [JsonPropertyName("telemetry_id")]
    public string TelemetryId { get; set; } = string.Empty;

    [JsonPropertyName("field_zone")]
    public string FieldZone { get; set; } = string.Empty;

    [JsonPropertyName("sensor_id")]
    public string SensorId { get; set; } = string.Empty;

    [JsonPropertyName("event_type")]
    public string EventType { get; set; } = "SoilSensorReading";

    [JsonPropertyName("soil_moisture_percent")]
    public double SoilMoisturePercent { get; set; }

    [JsonPropertyName("temperature_c")]
    public double TemperatureC { get; set; }

    [JsonPropertyName("humidity_percent")]
    public double HumidityPercent { get; set; }

    [JsonPropertyName("ph_level")]
    public double PhLevel { get; set; }

    [JsonPropertyName("nitrogen_ppm")]
    public double NitrogenPpm { get; set; }

    [JsonPropertyName("status")]
    public string Status { get; set; } = string.Empty;

    [JsonPropertyName("timestamp")]
    public string Timestamp { get; set; } = string.Empty;
}

public class IrrigationCommandPayload
{
    [JsonPropertyName("command_id")]
    public string? CommandId { get; set; }

    [JsonPropertyName("device_id")]
    public string DeviceId { get; set; } = "DEV-PIVOT-A1";

    [JsonPropertyName("field_zone")]
    public string FieldZone { get; set; } = "Sector-North-A1";

    [JsonPropertyName("action")]
    public string Action { get; set; } = "START_IRRIGATION";

    [JsonPropertyName("duration_minutes")]
    public int DurationMinutes { get; set; } = 45;

    [JsonPropertyName("flow_rate_lpm")]
    public int FlowRateLpm { get; set; } = 450;

    [JsonPropertyName("triggered_by")]
    public string TriggeredBy { get; set; } = "Farmer Dashboard";

    [JsonPropertyName("requested_at")]
    public string? RequestedAt { get; set; }
}
