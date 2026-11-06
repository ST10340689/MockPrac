-- ==============================================================================
-- AGRISTREAM IoT: AZURE SQL DATABASE RELATIONAL SCHEMA (cldvpracdb)
-- User Management Table (Username, Password, Role: Farmer, Agronomist, Admin)
-- Master Device Registry Table (Updated by Queue-Triggered Function)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. USER ACCOUNTS TABLE (Relational Database for User Management)
-- ------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Users')
BEGIN
    CREATE TABLE Users (
        UserId              INT IDENTITY(1,1)   NOT NULL,
        Username            NVARCHAR(64)        NOT NULL,
        Password            NVARCHAR(128)       NOT NULL,
        Role                NVARCHAR(32)        NOT NULL, -- 'Farmer', 'Agronomist', 'Admin'
        FullName            NVARCHAR(100)       NOT NULL,
        AssignedFarmZone    NVARCHAR(64)        NOT NULL DEFAULT 'All Zones',
        CreatedAt           DATETIMEOFFSET      NOT NULL DEFAULT SYSDATETIMEOFFSET(),

        CONSTRAINT PK_Users PRIMARY KEY CLUSTERED (UserId),
        CONSTRAINT UQ_Users_Username UNIQUE (Username),
        CONSTRAINT CK_Users_Role CHECK (Role IN ('Farmer', 'Agronomist', 'Admin'))
    );
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Users_Role' AND object_id = OBJECT_ID('Users'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_Users_Role
    ON Users (Role, Username)
    INCLUDE (FullName, AssignedFarmZone);
END
GO

-- Seed initial AgriStream IoT User Accounts (Farmer, Agronomist, Admin)
IF NOT EXISTS (SELECT 1 FROM Users WHERE Username = 'johan.venter')
BEGIN
    INSERT INTO Users (Username, Password, Role, FullName, AssignedFarmZone)
    VALUES
        ('johan.venter', 'FarmPass#2026', 'Farmer', 'Johan Venter', 'Sector-North-A1 (Maize & Pivot 1)'),
        ('pieter.mokoena', 'Harvest#2026', 'Farmer', 'Pieter Mokoena', 'Sector-South-B2 (Citrus Orchard)'),
        ('dr.thandi.nkosi', 'AgroSci#2026', 'Agronomist', 'Dr. Thandi Nkosi', 'Sector-East-C3 (Soil & NDVI Lab)'),
        ('anika.patel', 'SoilCare#2026', 'Agronomist', 'Anika Patel', 'Sector-West-D4 (Soybean & Drone Grid)'),
        ('admin.agristream', 'AdminRoot#2026', 'Admin', 'Riaz Hassim (Platform Admin)', 'All Commercial Farm Sectors');
END
GO

-- ------------------------------------------------------------------------------
-- 2. MASTER DEVICE REGISTRY TABLE (Updated by Queue-Triggered Function)
-- ------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DeviceRegistry')
BEGIN
    CREATE TABLE DeviceRegistry (
        DeviceId                NVARCHAR(64)        NOT NULL,
        DeviceName              NVARCHAR(100)       NOT NULL,
        FieldZone               NVARCHAR(64)        NOT NULL,
        DeviceType              NVARCHAR(64)        NOT NULL, -- 'IrrigationPivot', 'SoilSensorNode', 'FieldDrone'
        OperationalStatus       NVARCHAR(128)       NOT NULL DEFAULT 'Online - Monitoring',
        SoilMoisturePercent     DECIMAL(5, 2)       NOT NULL DEFAULT 42.50,
        TemperatureC            DECIMAL(5, 2)       NOT NULL DEFAULT 24.00,
        LastCommand             NVARCHAR(256)       NULL,
        LastUpdated             DATETIMEOFFSET      NOT NULL DEFAULT SYSDATETIMEOFFSET(),
        UpdatedByFunction       NVARCHAR(64)        NOT NULL DEFAULT 'SystemSeed',

        CONSTRAINT PK_DeviceRegistry PRIMARY KEY CLUSTERED (DeviceId)
    );
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_DeviceRegistry_FieldZone_Status' AND object_id = OBJECT_ID('DeviceRegistry'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_DeviceRegistry_FieldZone_Status
    ON DeviceRegistry (FieldZone, LastUpdated DESC)
    INCLUDE (DeviceName, OperationalStatus, SoilMoisturePercent, LastCommand);
END
GO

-- Seed initial commercial farm IoT devices and irrigation actuators
IF NOT EXISTS (SELECT 1 FROM DeviceRegistry WHERE DeviceId = 'DEV-PIVOT-A1')
BEGIN
    INSERT INTO DeviceRegistry (DeviceId, DeviceName, FieldZone, DeviceType, OperationalStatus, SoilMoisturePercent, TemperatureC, LastCommand, UpdatedByFunction)
    VALUES
        ('DEV-PIVOT-A1', 'Center Pivot Irrigator #1', 'Sector-North-A1', 'IrrigationPivot', 'Standby - Valve Closed', 21.40, 29.80, 'Awaiting Moisture Threshold Trigger', 'SystemInit'),
        ('DEV-PIVOT-B2', 'Drip Irrigation Manifold #2', 'Sector-South-B2', 'IrrigationPivot', 'Irrigating - Active (420 L/min)', 54.20, 24.50, 'START_IRRIGATION (Duration: 45m)', 'ProcessIrrigationQueue'),
        ('DEV-VALVE-C3', 'Smart Solenoid Valve Grid #3', 'Sector-East-C3', 'IrrigationPivot', 'Standby - Optimal Moisture', 61.80, 22.10, 'AUTO_HOLD', 'SystemInit'),
        ('DEV-PIVOT-D4', 'High-Pressure Boom Sprayer #4', 'Sector-West-D4', 'IrrigationPivot', 'Warning - Low Soil Moisture (18.2%)', 18.20, 31.40, 'ALERT_LOW_MOISTURE', 'SystemInit'),
        ('DEV-DRONE-01', 'AgriHawk Multispectral Drone X8', 'Sector-North-A1', 'FieldDrone', 'Active - Uploading Aerial NDVI Imagery', 21.40, 29.80, 'BLOB_UPLOAD_COMPLETED', 'DroneImageBlobTrigger');
END
GO

-- ==============================================================================
-- SAMPLE VERIFICATION QUERIES
-- ==============================================================================
-- 1. Verify User Credentials & Assigned Roles:
-- SELECT UserId, Username, Role, FullName, AssignedFarmZone, CreatedAt FROM Users ORDER BY UserId;

-- 2. Verify Master Device Registry Operational Status:
-- SELECT DeviceId, DeviceName, FieldZone, OperationalStatus, SoilMoisturePercent, LastCommand, LastUpdated FROM DeviceRegistry;
