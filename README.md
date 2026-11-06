# How to Run Locally on Windows

### Option A: One-Click Launcher
Double-click **`RUN_PROJECT.bat`** in the root folder to compile the solution, start the ASP.NET Core MVC Web Application on **`http://localhost:5000`**, open the live dashboard in your browser, and execute the 1,000-request concurrent load test.

### Option B: Via .NET CLI (PowerShell)
```powershell
# Terminal 1: Start the AgriStream IoT MVC Web Application
dotnet run --project src/Ecommerce.AppService/Ecommerce.AppService.csproj --urls "http://localhost:5000"

# Terminal 2: Run the High-Concurrency Load Test (1,000 requests, 50 concurrent workers)
dotnet run --project src/Ecommerce.StressTest/Ecommerce.StressTest.csproj -- "http://localhost:5000/api/telemetry" 1000 50
```
