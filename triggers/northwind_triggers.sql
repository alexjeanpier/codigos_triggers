USE Northwind;
GO

-- =============================================
-- 1. CREAR TABLAS AUXILIARES
-- =============================================
IF OBJECT_ID('ProductPriceAudit', 'U') IS NOT NULL DROP TABLE ProductPriceAudit;
CREATE TABLE ProductPriceAudit (
    AuditID INT IDENTITY(1,1) PRIMARY KEY,
    ProductID INT,
    ProductName NVARCHAR(40),
    OldPrice MONEY,
    NewPrice MONEY,
    ChangePercentage DECIMAL(5,2),
    ChangedBy NVARCHAR(100),
    ChangeDate DATETIME DEFAULT GETDATE()
);
GO

IF OBJECT_ID('EmployeeReportsToHistory', 'U') IS NOT NULL DROP TABLE EmployeeReportsToHistory;
CREATE TABLE EmployeeReportsToHistory (
    HistoryID INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID INT,
    EmployeeName NVARCHAR(100),
    OldBossID INT,
    OldBossName NVARCHAR(100),
    NewBossID INT,
    NewBossName NVARCHAR(100),
    ChangeDate DATETIME DEFAULT GETDATE()
);
GO

IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('Orders') AND name = 'OrderTotal')
    ALTER TABLE Orders ADD OrderTotal MONEY DEFAULT 0;
GO

-- =============================================
-- 2. TRIGGER 1: Control de Stock
-- =============================================
IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'trg_AutoUpdateStock')
    DROP TRIGGER trg_AutoUpdateStock;
GO

CREATE TRIGGER trg_AutoUpdateStock
ON [Order Details]
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE p SET p.UnitsInStock = p.UnitsInStock - i.Quantity
    FROM Products p
    INNER JOIN inserted i ON p.ProductID = i.ProductID
    WHERE p.Discontinued = 0;
END
GO

-- =============================================
-- 3. TRIGGER 2: Auditoría de Precios
-- =============================================
IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'trg_ProductPriceAudit')
    DROP TRIGGER trg_ProductPriceAudit;
GO

CREATE TRIGGER trg_ProductPriceAudit
ON Products
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF UPDATE(UnitPrice)
    BEGIN
        INSERT INTO ProductPriceAudit (ProductID, ProductName, OldPrice, NewPrice, ChangePercentage, ChangedBy)
        SELECT d.ProductID, d.ProductName, d.UnitPrice, i.UnitPrice,
               ROUND(((i.UnitPrice - d.UnitPrice) / NULLIF(d.UnitPrice, 0)) * 100, 2),
               SUSER_NAME()
        FROM deleted d
        INNER JOIN inserted i ON d.ProductID = i.ProductID
        WHERE d.UnitPrice <> i.UnitPrice;
    END
END
GO

-- =============================================
-- 4. TRIGGER 3: Validación de Pedidos
-- =============================================
IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'trg_OrderValidation')
    DROP TRIGGER trg_OrderValidation;
GO

CREATE TRIGGER trg_OrderValidation
ON Orders
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM inserted WHERE ShippedDate < OrderDate AND ShippedDate IS NOT NULL)
    BEGIN
        RAISERROR('ERROR: Fecha envío anterior a fecha pedido', 16, 1);
        ROLLBACK TRANSACTION;
    END
END
GO

-- =============================================
-- 5. TRIGGER 4: Histórico de Jefes
-- =============================================
IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'trg_EmployeeBossHistory')
    DROP TRIGGER trg_EmployeeBossHistory;
GO

CREATE TRIGGER trg_EmployeeBossHistory
ON Employees
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF UPDATE(ReportsTo)
    BEGIN
        INSERT INTO EmployeeReportsToHistory (EmployeeID, EmployeeName, OldBossID, OldBossName, NewBossID, NewBossName)
        SELECT d.EmployeeID, d.FirstName + ' ' + d.LastName,
               d.ReportsTo, 
               (SELECT FirstName + ' ' + LastName FROM Employees WHERE EmployeeID = d.ReportsTo),
               i.ReportsTo,
               (SELECT FirstName + ' ' + LastName FROM Employees WHERE EmployeeID = i.ReportsTo)
        FROM deleted d
        INNER JOIN inserted i ON d.EmployeeID = i.EmployeeID
        WHERE ISNULL(d.ReportsTo, -1) <> ISNULL(i.ReportsTo, -1);
    END
END
GO

-- =============================================
-- 6. TRIGGER 5: Sistema de Totalización
-- =============================================
IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'trg_OrderTotalSystem')
    DROP TRIGGER trg_OrderTotalSystem;
GO

CREATE TRIGGER trg_OrderTotalSystem
ON [Order Details]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE o
    SET o.OrderTotal = (
        SELECT ROUND(SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)), 2)
        FROM [Order Details] od
        WHERE od.OrderID = o.OrderID
    )
    FROM Orders o
    WHERE o.OrderID IN (SELECT OrderID FROM inserted UNION SELECT OrderID FROM deleted);
END
GO

-- =============================================
-- DEMOSTRACIÓN 1: TRIGGER DE STOCK
-- =============================================
SELECT '1. PRODUCTO ANTES' AS Info, ProductID, ProductName, UnitsInStock 
FROM Products WHERE ProductID = 17;

DECLARE @Order1 INT;
INSERT INTO Orders (CustomerID, EmployeeID, OrderDate, RequiredDate, Freight) 
VALUES ('ALFKI', 1, GETDATE(), DATEADD(day, 7, GETDATE()), 15.50);
SET @Order1 = SCOPE_IDENTITY();

INSERT INTO [Order Details] (OrderID, ProductID, UnitPrice, Quantity, Discount)
VALUES (@Order1, 17, 39.00, 5, 0);

SELECT '2. PRODUCTO DESPUÉS' AS Info, ProductID, ProductName, UnitsInStock 
FROM Products WHERE ProductID = 17;
GO

-- =============================================
-- DEMOSTRACIÓN 2: TRIGGER DE AUDITORÍA
-- =============================================
SELECT '1. PRECIO ANTES' AS Info, ProductID, ProductName, UnitPrice 
FROM Products WHERE ProductID = 1;

UPDATE Products SET UnitPrice = 25.00 WHERE ProductID = 1;

SELECT '2. AUDITORÍA' AS Info, ProductID, ProductName, OldPrice, NewPrice, ChangePercentage 
FROM ProductPriceAudit WHERE ProductID = 1;
GO

-- =============================================
-- DEMOSTRACIÓN 3: TRIGGER DE VALIDACIÓN
-- =============================================
SELECT '1. INTENTO FECHA INVÁLIDA' AS Info;

BEGIN TRY
    INSERT INTO Orders (CustomerID, OrderDate, ShippedDate, Freight)
    VALUES ('ALFKI', '2024-01-20', '2024-01-15', 25.00);
END TRY
BEGIN CATCH
    SELECT '2. ERROR CAPTURADO' AS Info, ERROR_MESSAGE() AS Mensaje;
END CATCH
GO

-- =============================================
-- DEMOSTRACIÓN 4: TRIGGER DE HISTÓRICO
-- =============================================
SELECT '1. EMPLEADO ANTES' AS Info, EmployeeID, FirstName + ' ' + LastName AS Empleado, ReportsTo 
FROM Employees WHERE EmployeeID = 3;

UPDATE Employees SET ReportsTo = 5 WHERE EmployeeID = 3;

SELECT '2. HISTÓRICO' AS Info, EmployeeID, EmployeeName, OldBossName, NewBossName 
FROM EmployeeReportsToHistory WHERE EmployeeID = 3;
GO

-- =============================================
-- DEMOSTRACIÓN 5: TRIGGER DE TOTALIZACIÓN
-- =============================================
DECLARE @Order2 INT;
INSERT INTO Orders (CustomerID, EmployeeID, OrderDate, RequiredDate, Freight) 
VALUES ('ALFKI', 1, GETDATE(), DATEADD(day, 7, GETDATE()), 20.00);
SET @Order2 = SCOPE_IDENTITY();

SELECT '1. PEDIDO SIN PRODUCTOS' AS Info, OrderID, OrderTotal 
FROM Orders WHERE OrderID = @Order2;

INSERT INTO [Order Details] (OrderID, ProductID, UnitPrice, Quantity, Discount)
VALUES (@Order2, 11, 14.00, 10, 0);

SELECT '2. PEDIDO CON PRODUCTOS' AS Info, OrderID, OrderTotal 
FROM Orders WHERE OrderID = @Order2;

SELECT '3. DETALLE PEDIDO' AS Info, OrderID, ProductID, Quantity, UnitPrice, 
       ROUND(UnitPrice * Quantity * (1 - Discount), 2) AS Subtotal
FROM [Order Details] WHERE OrderID = @Order2;
GO