@echo off
title AgriStream IoT — Real-Time Processing System Launcher (ASP.NET Core MVC)
echo ======================================================================
echo   STARTING AGRISTREAM IoT REAL-TIME PROCESSING SYSTEM (C# .NET MVC)
echo ======================================================================
echo.
set "PATH=C:\Users\riazh\AppData\Local\Microsoft\dotnet;C:\Users\USER\.gemini\antigravity\brain\fef3b6e0-e8ef-4e29-84de-9a7e75f0d2ff\scratch\dotnet;C:\Program Files\dotnet;%PATH%"

echo [1/3] Building AgriStream IoT Solution (MVC App, 4 Azure Functions, Load Tester)...
dotnet build "%~dp0EcommerceRealTimeSystem.sln" --nologo -v q

echo [2/3] Starting AgriStream IoT MVC Web Application on http://localhost:5000 ...
start "AgriStream.MVC (http://localhost:5000)" dotnet "%~dp0src\Ecommerce.AppService\bin\Debug\net9.0\Ecommerce.AppService.dll" --urls "http://localhost:5000"

timeout /t 3 /nobreak >nul

echo [3/3] Opening AgriStream IoT Live MVC Dashboard in Browser (http://localhost:5000)...
start "" "http://localhost:5000"

echo.
echo ======================================================================
echo   AGRISTREAM IoT MVC DASHBOARD IS RUNNING AT: http://localhost:5000
echo   Now running 1,000-Request Concurrent Load Test...
echo ======================================================================
echo.
dotnet "%~dp0src\Ecommerce.StressTest\bin\Debug\net9.0\Ecommerce.StressTest.dll" "http://localhost:5000/api/telemetry" 1000 50
pause
