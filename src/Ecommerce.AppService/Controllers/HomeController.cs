using Ecommerce.AppService.Models;
using Ecommerce.AppService.Services;
using Microsoft.AspNetCore.Mvc;

namespace Ecommerce.AppService.Controllers;

public class HomeController : Controller
{
    private readonly AgriStreamDataService _dataService;

    public HomeController(AgriStreamDataService dataService)
    {
        _dataService = dataService;
    }

    // =========================================================================
    // Main MVC Management Web Application Dashboard View (GET /)
    // =========================================================================
    [HttpGet("/")]
    [HttpGet("/Home")]
    [HttpGet("/Home/Index")]
    public IActionResult Index()
    {
        var vm = _dataService.BuildDashboardViewModel();
        return View(vm);
    }

    // =========================================================================
    // Relational Database (User Management in Azure SQL Database)
    // =========================================================================
    [HttpGet("/api/users")]
    public async Task<IActionResult> GetUsers()
    {
        var users = await _dataService.GetUsersAsync();
        return Ok(new
        {
            source = "Azure SQL Database (cldvpracdb.dbo.Users)",
            count = users.Count,
            users
        });
    }

    [HttpPost("/api/users")]
    public async Task<IActionResult> CreateUser([FromBody] UserAccount req)
    {
        var created = await _dataService.RegisterUserAsync(req);
        return Ok(new
        {
            status = "Created",
            database = "Azure SQL Database (cldvpracdb.dbo.Users)",
            user = created
        });
    }

    [HttpPost("/api/users/login")]
    public IActionResult Login([FromBody] LoginRequest req)
    {
        var user = _dataService.AuthenticateUser(req.Username, req.Password);
        if (user == null)
        {
            return Unauthorized(new { status = "Invalid credentials" });
        }

        return Ok(new
        {
            status = "Authenticated",
            database = "Azure SQL Database (cldvpracdb.dbo.Users)",
            user
        });
    }

    // =========================================================================
    // Azure Blob Storage Upload & BlobTrigger Audit
    // =========================================================================
    [HttpPost("/api/drones/upload")]
    public async Task<IActionResult> UploadDroneImage(
        [FromForm] IFormFile? imageFile,
        [FromForm] string? fieldZone,
        [FromForm] string? droneId)
    {
        byte[] fileBytes = Array.Empty<byte>();
        string fileName = $"drone-{DateTime.UtcNow:HHmmss}.svg";
        string contentType = "image/svg+xml";

        if (imageFile != null && imageFile.Length > 0)
        {
            using var ms = new MemoryStream();
            await imageFile.CopyToAsync(ms);
            fileBytes = ms.ToArray();
            fileName = imageFile.FileName;
            contentType = string.IsNullOrWhiteSpace(imageFile.ContentType) ? "image/png" : imageFile.ContentType;
        }

        var audit = await _dataService.UploadDroneImageAndTriggerBlobFunctionAsync(
            fileName,
            contentType,
            fileBytes,
            fieldZone ?? "Sector-North-A1",
            droneId ?? "DRONE-AGRI-X8");

        return Ok(new
        {
            status = "Uploaded & Audited",
            blob_container = "cldvpracstorage/drone-images",
            triggered_function = "Function 1: DroneImageBlobTrigger (BlobTrigger -> Table Storage Audit)",
            audit
        });
    }

    [HttpGet("/api/drones/blob/{blobName}")]
    public IActionResult GetDroneBlob(string blobName)
    {
        var (bytes, contentType) = _dataService.GetDroneBlobBytes(blobName);
        if (bytes == null)
        {
            return NotFound();
        }
        return File(bytes, contentType);
    }

    [HttpGet("/api/drones/audits")]
    public IActionResult GetDroneAudits()
    {
        var audits = _dataService.GetDroneAudits();
        return Ok(new
        {
            source = "Azure Table Storage (cldvpracstorage -> DroneImageAudit)",
            function_trigger = "DroneImageBlobTrigger [BlobTrigger(\"drone-images/{name}\")]",
            count = audits.Count,
            audits
        });
    }

    // =========================================================================
    // HTTP-Triggered Function (GET)
    // Called when a farmer opens their field dashboard to retrieve latest
    // telemetry data from Azure Table Storage for near-instant rendering.
    // =========================================================================
    [HttpGet("/api/functions/telemetry")]
    [HttpGet("/api/telemetry")]
    public IActionResult GetLatestTelemetry([FromQuery] string? fieldZone = null)
    {
        var telemetry = _dataService.GetLatestTelemetryFromTableStorage(fieldZone);
        var audits = _dataService.GetDroneAudits();
        return Ok(new
        {
            status = "Success",
            function_name = "Function 2: GetLatestTelemetry [HttpTrigger(GET, Route = \"telemetry\")]",
            source = "Azure Table Storage (cldvpracstorage -> SensorTelemetry)",
            count = telemetry.Count,
            telemetry,
            drone_audits = audits
        });
    }

    [HttpPost("/api/telemetry")]
    [HttpPost("/api/orders")]
    public async Task<IActionResult> IngestTelemetry([FromBody] SensorTelemetryDto? payload)
    {
        payload ??= new SensorTelemetryDto();
        var saved = await _dataService.IngestSensorTelemetryAsync(payload);
        return Accepted($"/api/telemetry?fieldZone={saved.FieldZone}", new
        {
            status = "Accepted",
            telemetry_id = saved.TelemetryId,
            storage = "Azure Table Storage (cldvpracstorage -> SensorTelemetry)",
            ingested_at = saved.Timestamp
        });
    }

    // =========================================================================
    // HTTP POST Trigger & Queue-Triggered Function:
    // Places an "Irrigation Command" into Azure Storage Queue ("irrigation-commands")
    // and processes it via the Queue-Triggered Function to update the device's
    // operational status in the master DeviceRegistry table in Azure SQL Database.
    // =========================================================================
    [HttpPost("/api/functions/irrigation/trigger")]
    [HttpPost("/api/irrigation/trigger")]
    public async Task<IActionResult> TriggerIrrigation([FromBody] IrrigationCommandDto? req)
    {
        req ??= new IrrigationCommandDto();
        var (cmd, updatedDevice, enqueued) = await _dataService.TriggerIrrigationPipelineAsync(req);

        return Accepted("/api/devices", new
        {
            status = "Processed",
            http_post_function = "Function 3: TriggerIrrigationCommand [HttpTrigger(POST)] -> Enqueued to 'irrigation-commands'",
            queue_trigger_function = "Function 4: ProcessIrrigationQueue [QueueTrigger(\"irrigation-commands\")] -> Updated Azure SQL DeviceRegistry",
            enqueued_to_azure_queue = enqueued,
            command = cmd,
            updated_device = updatedDevice
        });
    }

    [HttpGet("/api/devices")]
    public IActionResult GetDevices()
    {
        var devices = _dataService.GetDevices();
        return Ok(new
        {
            source = "Azure SQL Database (cldvpracdb.dbo.DeviceRegistry)",
            updated_by = "Function 4: ProcessIrrigationQueue [QueueTrigger(\"irrigation-commands\")]",
            count = devices.Count,
            devices
        });
    }

    // =========================================================================
    // Health, Monitoring Metrics & Auto-Scale Simulation
    // =========================================================================
    [HttpGet("/api/health")]
    public IActionResult Health()
    {
        return Ok(new
        {
            status = "Healthy",
            application = "AgriStream IoT MVC Management Web Application",
            azure_sql_database = "cldvprac.database.windows.net / cldvpracdb (Users & DeviceRegistry)",
            azure_storage = "cldvpracstorage (Blob: drone-images, Table: SensorTelemetry & DroneImageAudit, Queue: irrigation-commands)",
            azure_functions = new[]
            {
                "1. DroneImageBlobTrigger [BlobTrigger(\"drone-images/{name}\")]",
                "2. GetLatestTelemetry [HttpTrigger(GET, Route = \"telemetry\")]",
                "3. TriggerIrrigationCommand [HttpTrigger(POST, Route = \"irrigation/trigger\")]",
                "4. ProcessIrrigationQueue [QueueTrigger(\"irrigation-commands\")]"
            }
        });
    }

    [HttpGet("/api/metrics")]
    public IActionResult Metrics()
    {
        return Ok(_dataService.GetSystemMetrics());
    }

    [HttpPost("/api/simulate-cpu-load")]
    public IActionResult SimulateCpuLoad([FromBody] CpuSimulationRequest? req)
    {
        req ??= new CpuSimulationRequest();
        return Ok(_dataService.SimulateCpuAndMemoryScaleOut(req));
    }
}
