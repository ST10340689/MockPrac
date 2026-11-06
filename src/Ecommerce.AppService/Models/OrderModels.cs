using System.Text.Json.Serialization;

namespace Ecommerce.AppService.Models;

// ============================================================================
// Relational Database Models — User Management (Azure SQL Database)
// ============================================================================
public class UserAccount
{
    [JsonPropertyName("user_id")]
    public int UserId { get; set; }

    [JsonPropertyName("username")]
    public string Username { get; set; } = string.Empty;

    [JsonPropertyName("password")]
    public string Password { get; set; } = string.Empty;

    [JsonPropertyName("role")]
    public string Role { get; set; } = "Farmer"; // Farmer, Agronomist, Admin

    [JsonPropertyName("full_name")]
    public string FullName { get; set; } = string.Empty;

    [JsonPropertyName("assigned_farm_zone")]
    public string AssignedFarmZone { get; set; } = "Sector-North-A1";

    [JsonPropertyName("created_at")]
    public string CreatedAt { get; set; } = DateTime.UtcNow.ToString("o");
}

public class LoginRequest
{
    [JsonPropertyName("username")]
    public string Username { get; set; } = string.Empty;

    [JsonPropertyName("password")]
    public string Password { get; set; } = string.Empty;
}

// ============================================================================
// Azure Table Storage Models — High-Frequency Sensor Telemetry
// ============================================================================
public class SensorTelemetryDto
{
    [JsonPropertyName("telemetry_id")]
    public string? TelemetryId { get; set; }

    [JsonPropertyName("field_zone")]
    public string FieldZone { get; set; } = "Sector-North-A1";

    [JsonPropertyName("sensor_id")]
    public string SensorId { get; set; } = "SENS-SOIL-101";

    [JsonPropertyName("event_type")]
    public string EventType { get; set; } = "SoilSensorReading"; // SoilSensorReading, IrrigationEvent, EnvironmentalLog

    [JsonPropertyName("soil_moisture_percent")]
    public double SoilMoisturePercent { get; set; } = 38.5;

    [JsonPropertyName("temperature_c")]
    public double TemperatureC { get; set; } = 25.4;

    [JsonPropertyName("humidity_percent")]
    public double HumidityPercent { get; set; } = 54.0;

    [JsonPropertyName("ph_level")]
    public double PhLevel { get; set; } = 6.5;

    [JsonPropertyName("nitrogen_ppm")]
    public double NitrogenPpm { get; set; } = 42.0;

    [JsonPropertyName("status")]
    public string Status { get; set; } = "Optimal";

    [JsonPropertyName("timestamp")]
    public string? Timestamp { get; set; }
}

// ============================================================================
// Azure Blob Storage & Blob-Triggered Function Audit Models
// ============================================================================
public class DroneImageAuditDto
{
    [JsonPropertyName("audit_id")]
    public string AuditId { get; set; } = string.Empty;

    [JsonPropertyName("field_zone")]
    public string FieldZone { get; set; } = "Sector-North-A1";

    [JsonPropertyName("drone_id")]
    public string DroneId { get; set; } = "DRONE-AGRI-X8";

    [JsonPropertyName("blob_name")]
    public string BlobName { get; set; } = string.Empty;

    [JsonPropertyName("blob_url")]
    public string BlobUrl { get; set; } = string.Empty;

    [JsonPropertyName("file_size_bytes")]
    public long FileSizeBytes { get; set; }

    [JsonPropertyName("content_type")]
    public string ContentType { get; set; } = "image/svg+xml";

    [JsonPropertyName("processing_status")]
    public string ProcessingStatus { get; set; } = "Processed by BlobTrigger -> Logged to Azure Table Storage";

    [JsonPropertyName("ndvi_analysis")]
    public string NdviAnalysis { get; set; } = "NDVI 0.78 (Healthy Canopy)";

    [JsonPropertyName("uploaded_at")]
    public string UploadedAt { get; set; } = DateTime.UtcNow.ToString("o");
}

// ============================================================================
// HTTP POST Trigger -> Azure Storage Queue -> Queue Trigger -> SQL
// ============================================================================
public class IrrigationCommandDto
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
    public string TriggeredBy { get; set; } = "Farmer Dashboard (Manual Trigger)";

    [JsonPropertyName("requested_at")]
    public string? RequestedAt { get; set; }
}

public class DeviceRegistryDto
{
    [JsonPropertyName("device_id")]
    public string DeviceId { get; set; } = string.Empty;

    [JsonPropertyName("device_name")]
    public string DeviceName { get; set; } = string.Empty;

    [JsonPropertyName("field_zone")]
    public string FieldZone { get; set; } = string.Empty;

    [JsonPropertyName("device_type")]
    public string DeviceType { get; set; } = "IrrigationPivot";

    [JsonPropertyName("operational_status")]
    public string OperationalStatus { get; set; } = "Online - Monitoring";

    [JsonPropertyName("soil_moisture_percent")]
    public decimal SoilMoisturePercent { get; set; }

    [JsonPropertyName("temperature_c")]
    public decimal TemperatureC { get; set; }

    [JsonPropertyName("last_command")]
    public string LastCommand { get; set; } = string.Empty;

    [JsonPropertyName("last_updated")]
    public string LastUpdated { get; set; } = DateTime.UtcNow.ToString("o");

    [JsonPropertyName("updated_by_function")]
    public string UpdatedByFunction { get; set; } = "ProcessIrrigationQueue";
}

// ============================================================================
// MVC ViewModel for AgriStream IoT Management Web Application
// ============================================================================
public class AgriStreamDashboardViewModel
{
    public List<UserAccount> Users { get; set; } = new();
    public List<DeviceRegistryDto> Devices { get; set; } = new();
    public List<SensorTelemetryDto> TelemetryFeed { get; set; } = new();
    public List<DroneImageAuditDto> DroneAudits { get; set; } = new();
    public UserAccount? CurrentUser { get; set; }
    public string AzureSqlServer { get; set; } = "cldvprac.database.windows.net / cldvpracdb";
    public string AzureStorageAccount { get; set; } = "cldvpracstorage (Blob: drone-images, Table: SensorTelemetry, Queue: irrigation-commands)";
    public bool IsSqlLiveConnected { get; set; }
    public bool IsStorageLiveConnected { get; set; }
    public int ActiveAppServiceInstances { get; set; } = 1;
    public int ActiveFunctionInstances { get; set; } = 4;
    public double CpuPercentage { get; set; } = 21.4;
    public double MemoryPercentage { get; set; } = 44.2;
    public long TotalRequests { get; set; }
}

public class CpuSimulationRequest
{
    [JsonPropertyName("duration_seconds")]
    public int DurationSeconds { get; set; } = 3;

    [JsonPropertyName("intensity_factor")]
    public int IntensityFactor { get; set; } = 500_000;
}
