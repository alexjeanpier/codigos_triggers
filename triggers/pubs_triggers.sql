/* ============================================= */
/* TRIGGERS PARA LA BASE DE DATOS PUBS EXISTENTE */
/* ============================================= */

-- Verificar si la base de datos pubs existe
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'pubs')
BEGIN
    PRINT 'La base de datos pubs no existe. Creándola primero...';
    
    -- Crear la base de datos (esto requiere estar en master)
    USE master;
    
    -- Eliminar conexiones existentes primero
    ALTER DATABASE pubs SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE IF EXISTS pubs;
    
    -- Crear nueva base de datos
    CREATE DATABASE pubs;
    
    PRINT 'Base de datos pubs creada exitosamente.';
    PRINT 'Ejecuta primero el script original de pubs para crear las tablas y datos.';
    PRINT 'Luego ejecuta este script de triggers.';
    RETURN;
END
ELSE
BEGIN
    PRINT 'Base de datos pubs encontrada. Procediendo a crear triggers...';
    
    -- Usar la base de datos pubs
    USE pubs;
END
GO

/* ============================================= */
/* ELIMINAR TRIGGERS EXISTENTES                  */
/* ============================================= */

PRINT 'Eliminando triggers existentes...';

IF OBJECT_ID('trg_update_ytd_sales', 'TR') IS NOT NULL 
BEGIN
    DROP TRIGGER trg_update_ytd_sales;
    PRINT 'Trigger trg_update_ytd_sales eliminado.';
END

IF OBJECT_ID('trg_validate_title_price', 'TR') IS NOT NULL 
BEGIN
    DROP TRIGGER trg_validate_title_price;
    PRINT 'Trigger trg_validate_title_price eliminado.';
END

IF OBJECT_ID('trg_authors_audit', 'TR') IS NOT NULL 
BEGIN
    DROP TRIGGER trg_authors_audit;
    PRINT 'Trigger trg_authors_audit eliminado.';
END

IF OBJECT_ID('trg_validate_royaltyper', 'TR') IS NOT NULL 
BEGIN
    DROP TRIGGER trg_validate_royaltyper;
    PRINT 'Trigger trg_validate_royaltyper eliminado.';
END

IF OBJECT_ID('trg_price_history', 'TR') IS NOT NULL 
BEGIN
    DROP TRIGGER trg_price_history;
    PRINT 'Trigger trg_price_history eliminado.';
END

IF OBJECT_ID('trg_validate_discounts', 'TR') IS NOT NULL 
BEGIN
    DROP TRIGGER trg_validate_discounts;
    PRINT 'Trigger trg_validate_discounts eliminado.';
END

IF OBJECT_ID('trg_prevent_publisher_delete', 'TR') IS NOT NULL 
BEGIN
    DROP TRIGGER trg_prevent_publisher_delete;
    PRINT 'Trigger trg_prevent_publisher_delete eliminado.';
END

IF OBJECT_ID('trg_validate_employee_job', 'TR') IS NOT NULL 
BEGIN
    DROP TRIGGER trg_validate_employee_job;
    PRINT 'Trigger trg_validate_employee_job eliminado.';
END

IF OBJECT_ID('trg_calculate_total_royalty', 'TR') IS NOT NULL 
BEGIN
    DROP TRIGGER trg_calculate_total_royalty;
    PRINT 'Trigger trg_calculate_total_royalty eliminado.';
END

PRINT 'Todos los triggers anteriores eliminados.';
GO

/* ============================================= */
/* CREAR TABLAS AUXILIARES PARA LOS TRIGGERS     */
/* ============================================= */

PRINT 'Creando tablas auxiliares para los triggers...';

-- Tabla para auditoría de autores
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'authors_audit')
BEGIN
    CREATE TABLE authors_audit (
        audit_id INT IDENTITY(1,1) PRIMARY KEY,
        au_id VARCHAR(11),
        action_type VARCHAR(10),
        action_date DATETIME DEFAULT GETDATE(),
        user_name VARCHAR(100) DEFAULT SYSTEM_USER,
        notes VARCHAR(200)
    );
    PRINT 'Tabla authors_audit creada exitosamente.';
END
ELSE
BEGIN
    PRINT 'Tabla authors_audit ya existe.';
END
GO

-- Tabla para historial de precios
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'price_history')
BEGIN
    CREATE TABLE price_history (
        history_id INT IDENTITY(1,1) PRIMARY KEY,
        title_id VARCHAR(6),
        old_price MONEY,
        new_price MONEY,
        change_date DATETIME DEFAULT GETDATE(),
        changed_by VARCHAR(100) DEFAULT SYSTEM_USER
    );
    PRINT 'Tabla price_history creada exitosamente.';
END
ELSE
BEGIN
    PRINT 'Tabla price_history ya existe.';
END
GO

/* ============================================= */
/* CREAR LOS TRIGGERS                            */
/* ============================================= */

PRINT 'Creando nuevos triggers...';

/* ============================================= */
/* 1. TRIGGER: Actualizar Ventas Automáticamente */
/* ============================================= */
CREATE TRIGGER trg_update_ytd_sales
ON sales
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @title_id VARCHAR(6);
    DECLARE @qty INT;
    DECLARE @old_sales INT;
    
    DECLARE sales_cursor CURSOR FOR
    SELECT title_id, qty FROM inserted;
    
    OPEN sales_cursor;
    FETCH NEXT FROM sales_cursor INTO @title_id, @qty;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Obtener ventas actuales
        SELECT @old_sales = ISNULL(ytd_sales, 0) FROM titles WHERE title_id = @title_id;
        
        -- Actualizar ventas
        UPDATE titles 
        SET ytd_sales = @old_sales + @qty
        WHERE title_id = @title_id;
        
        PRINT 'Título ' + @title_id + ': Ventas actualizadas de ' + 
              CAST(@old_sales AS VARCHAR) + ' a ' + 
              CAST((@old_sales + @qty) AS VARCHAR);
        
        FETCH NEXT FROM sales_cursor INTO @title_id, @qty;
    END
    
    CLOSE sales_cursor;
    DEALLOCATE sales_cursor;
END
GO

PRINT 'Trigger trg_update_ytd_sales creado exitosamente.';
GO

/* ============================================= */
/* 2. TRIGGER: Validar Precios de Títulos        */
/* ============================================= */
CREATE TRIGGER trg_validate_title_price
ON titles
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Validar que el precio no sea negativo
    IF EXISTS (SELECT 1 FROM inserted WHERE price < 0)
    BEGIN
        RAISERROR('ERROR: El precio no puede ser negativo.', 16, 1);
        ROLLBACK TRANSACTION;
        RETURN;
    END
    
    -- Validar que el avance no sea negativo
    IF EXISTS (SELECT 1 FROM inserted WHERE advance < 0)
    BEGIN
        RAISERROR('ERROR: El avance no puede ser negativo.', 16, 1);
        ROLLBACK TRANSACTION;
        RETURN;
    END
    
    -- Validar que royalty esté entre 0 y 100
    IF EXISTS (SELECT 1 FROM inserted WHERE royalty NOT BETWEEN 0 AND 100 AND royalty IS NOT NULL)
    BEGIN
        RAISERROR('ERROR: El royalty debe estar entre 0 y 100.', 16, 1);
        ROLLBACK TRANSACTION;
        RETURN;
    END
    
    PRINT 'Validación de precios completada exitosamente.';
END
GO

PRINT 'Trigger trg_validate_title_price creado exitosamente.';
GO

/* ============================================= */
/* 3. TRIGGER: Auditoría de Autores              */
/* ============================================= */
CREATE TRIGGER trg_authors_audit
ON authors
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Para inserciones
    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
    BEGIN
        INSERT INTO authors_audit (au_id, action_type, notes)
        SELECT au_id, 'INSERT', 'Nuevo autor: ' + au_fname + ' ' + au_lname
        FROM inserted;
        PRINT 'Auditoría: Inserción registrada.';
    END
    
    -- Para actualizaciones
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
    BEGIN
        INSERT INTO authors_audit (au_id, action_type, notes)
        SELECT i.au_id, 'UPDATE', 'Autor actualizado: ' + i.au_fname + ' ' + i.au_lname
        FROM inserted i;
        PRINT 'Auditoría: Actualización registrada.';
    END
    
    -- Para eliminaciones
    IF EXISTS (SELECT * FROM deleted) AND NOT EXISTS (SELECT * FROM inserted)
    BEGIN
        INSERT INTO authors_audit (au_id, action_type, notes)
        SELECT au_id, 'DELETE', 'Autor eliminado'
        FROM deleted;
        PRINT 'Auditoría: Eliminación registrada.';
    END
END
GO

PRINT 'Trigger trg_authors_audit creado exitosamente.';
GO

/* ============================================= */
/* 4. TRIGGER: Validar Royalties en TitleAuthor  */
/* ============================================= */
CREATE TRIGGER trg_validate_royaltyper
ON titleauthor
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Validar que royaltyper esté entre 0 y 100
    IF EXISTS (SELECT 1 FROM inserted WHERE royaltyper NOT BETWEEN 0 AND 100)
    BEGIN
        RAISERROR('ERROR: El porcentaje de regalías debe estar entre 0 y 100.', 16, 1);
        ROLLBACK TRANSACTION;
        RETURN;
    END
    
    DECLARE @title_id VARCHAR(6);
    DECLARE cursor_titles CURSOR FOR
        SELECT DISTINCT title_id FROM inserted;
    
    OPEN cursor_titles;
    FETCH NEXT FROM cursor_titles INTO @title_id;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Calcular total de royalties para este título
        DECLARE @total_royalty INT;
        SELECT @total_royalty = SUM(royaltyper)
        FROM titleauthor
        WHERE title_id = @title_id;
        
        -- Validar que no exceda 100%
        IF @total_royalty > 100
        BEGIN
            RAISERROR('ERROR: Total de regalías para título %s = %d%%. No puede exceder 100%%.', 
                      16, 1, @title_id, @total_royalty);
            ROLLBACK TRANSACTION;
            CLOSE cursor_titles;
            DEALLOCATE cursor_titles;
            RETURN;
        END
        
        PRINT 'Título ' + @title_id + ': Total royalties = ' + CAST(ISNULL(@total_royalty, 0) AS VARCHAR) + '%';
        
        FETCH NEXT FROM cursor_titles INTO @title_id;
    END
    
    CLOSE cursor_titles;
    DEALLOCATE cursor_titles;
END
GO

PRINT 'Trigger trg_validate_royaltyper creado exitosamente.';
GO

/* ============================================= */
/* 5. TRIGGER: Historial de Cambios de Precio    */
/* ============================================= */
CREATE TRIGGER trg_price_history
ON titles
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Registrar solo cambios en el precio
    INSERT INTO price_history (title_id, old_price, new_price)
    SELECT d.title_id, d.price, i.price
    FROM deleted d
    INNER JOIN inserted i ON d.title_id = i.title_id
    WHERE (d.price <> i.price) 
       OR (d.price IS NULL AND i.price IS NOT NULL) 
       OR (d.price IS NOT NULL AND i.price IS NULL);
    
    DECLARE @count INT = @@ROWCOUNT;
    IF @count > 0
        PRINT 'Historial: ' + CAST(@count AS VARCHAR) + ' cambios de precio registrados.';
END
GO

PRINT 'Trigger trg_price_history creado exitosamente.';
GO

/* ============================================= */
/* 6. TRIGGER: Validar Descuentos                */
/* ============================================= */
CREATE TRIGGER trg_validate_discounts
ON discounts
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Validar rango de cantidades (lowqty <= highqty)
    IF EXISTS (SELECT 1 FROM inserted WHERE lowqty > highqty AND highqty IS NOT NULL)
    BEGIN
        RAISERROR('ERROR: lowqty no puede ser mayor que highqty.', 16, 1);
        ROLLBACK TRANSACTION;
        RETURN;
    END
    
    -- Validar porcentaje de descuento (0-100)
    IF EXISTS (SELECT 1 FROM inserted WHERE discount NOT BETWEEN 0 AND 100)
    BEGIN
        RAISERROR('ERROR: El descuento debe estar entre 0 y 100 por ciento.', 16, 1);
        ROLLBACK TRANSACTION;
        RETURN;
    END
    
    PRINT 'Validación de descuentos completada.';
END
GO

PRINT 'Trigger trg_validate_discounts creado exitosamente.';
GO

/* ============================================= */
/* 7. TRIGGER: Prevenir Eliminación de Publishers*/
/* ============================================= */
CREATE TRIGGER trg_prevent_publisher_delete
ON publishers
INSTEAD OF DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @pub_id CHAR(4);
    DECLARE @pub_name VARCHAR(40);
    DECLARE @book_count INT;
    
    DECLARE publisher_cursor CURSOR FOR
    SELECT d.pub_id, d.pub_name
    FROM deleted d;
    
    OPEN publisher_cursor;
    FETCH NEXT FROM publisher_cursor INTO @pub_id, @pub_name;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Contar libros del publisher
        SELECT @book_count = COUNT(*)
        FROM titles
        WHERE pub_id = @pub_id;
        
        IF @book_count > 0
        BEGIN
            PRINT 'ADVERTENCIA: No se puede eliminar ' + @pub_name + 
                  ' (ID: ' + @pub_id + ') porque tiene ' + 
                  CAST(@book_count AS VARCHAR) + ' libros asociados.';
        END
        ELSE
        BEGIN
            -- Eliminar publisher si no tiene libros
            DELETE FROM publishers WHERE pub_id = @pub_id;
            PRINT 'Publisher ' + @pub_name + ' eliminado exitosamente.';
        END
        
        FETCH NEXT FROM publisher_cursor INTO @pub_id, @pub_name;
    END
    
    CLOSE publisher_cursor;
    DEALLOCATE publisher_cursor;
END
GO

PRINT 'Trigger trg_prevent_publisher_delete creado exitosamente.';
GO

/* ============================================= */
/* 8. TRIGGER: Validar Empleados y Trabajos      */
/* ============================================= */
CREATE TRIGGER trg_validate_employee_job
ON employee
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @job_id SMALLINT;
    DECLARE @job_lvl TINYINT;
    DECLARE @min_lvl TINYINT;
    DECLARE @max_lvl TINYINT;
    DECLARE @job_desc VARCHAR(50);
    
    DECLARE emp_cursor CURSOR FOR
    SELECT i.job_id, i.job_lvl, j.min_lvl, j.max_lvl, j.job_desc
    FROM inserted i
    INNER JOIN jobs j ON i.job_id = j.job_id;
    
    OPEN emp_cursor;
    FETCH NEXT FROM emp_cursor INTO @job_id, @job_lvl, @min_lvl, @max_lvl, @job_desc;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Validar nivel del empleado
        IF @job_lvl NOT BETWEEN @min_lvl AND @max_lvl
        BEGIN
            RAISERROR('ERROR: Nivel %d no válido para trabajo "%s" (rango: %d-%d).', 
                      16, 1, @job_lvl, @job_desc, @min_lvl, @max_lvl);
            ROLLBACK TRANSACTION;
            CLOSE emp_cursor;
            DEALLOCATE emp_cursor;
            RETURN;
        END
        
        FETCH NEXT FROM emp_cursor INTO @job_id, @job_lvl, @min_lvl, @max_lvl, @job_desc;
    END
    
    CLOSE emp_cursor;
    DEALLOCATE emp_cursor;
    
    PRINT 'Validación de empleados completada.';
END
GO

PRINT 'Trigger trg_validate_employee_job creado exitosamente.';
GO

/* ============================================= */
/* 9. TRIGGER: Calcular Royalty Total por Título */
/* ============================================= */
CREATE TRIGGER trg_calculate_total_royalty
ON titleauthor
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Obtener títulos afectados
    DECLARE @title_id VARCHAR(6);
    DECLARE cursor_titles CURSOR FOR
        SELECT DISTINCT title_id FROM (
            SELECT title_id FROM inserted
            UNION
            SELECT title_id FROM deleted
        ) AS affected_titles;
    
    OPEN cursor_titles;
    FETCH NEXT FROM cursor_titles INTO @title_id;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Calcular y mostrar total actual
        DECLARE @total_royalty INT;
        DECLARE @author_count INT;
        
        SELECT @total_royalty = SUM(royaltyper),
               @author_count = COUNT(*)
        FROM titleauthor
        WHERE title_id = @title_id;
        
        PRINT 'Título ' + @title_id + ': ' + 
              CAST(@author_count AS VARCHAR) + ' autores, ' +
              'Total royalties: ' + CAST(ISNULL(@total_royalty, 0) AS VARCHAR) + '%';
        
        FETCH NEXT FROM cursor_titles INTO @title_id;
    END
    
    CLOSE cursor_titles;
    DEALLOCATE cursor_titles;
END
GO

PRINT 'Trigger trg_calculate_total_royalty creado exitosamente.';
GO

/* ============================================= */
/* PRUEBAS DE TRIGGERS - MOSTRANDO CAMBIOS       */
/* ============================================= */

USE pubs;
GO

PRINT '=== PRUEBAS DE TRIGGERS - DEMOSTRACIÓN DE CAMBIOS ===';
PRINT '';

/* ============================================= */
/* PRUEBA 1: Actualizar Ventas Automáticamente   */
/* ============================================= */
PRINT '=== 1. PRUEBA: Actualizar ventas automáticamente ===';
PRINT '';

-- Mostrar ventas actuales
PRINT 'ANTES - Ventas actuales para PS2091:';
SELECT title_id, title, ytd_sales 
FROM titles 
WHERE title_id = 'PS2091';
PRINT '';

-- Insertar nueva venta
PRINT 'INSERTANDO nueva venta...';
INSERT INTO sales (stor_id, ord_num, ord_date, qty, payterms, title_id)
VALUES ('7066', 'TEST001', GETDATE(), 10, 'Net 30', 'PS2091');
PRINT '';

-- Mostrar ventas después
PRINT 'DESPUÉS - Ventas actualizadas para PS2091:';
SELECT title_id, title, ytd_sales 
FROM titles 
WHERE title_id = 'PS2091';
PRINT '';

-- Limpiar
DELETE FROM sales WHERE ord_num = 'TEST001';
PRINT 'Datos de prueba limpiados.';
PRINT '----------------------------------------';
GO

/* ============================================= */
/* PRUEBA 2: Auditoría de Autores                */
/* ============================================= */
PRINT '=== 2. PRUEBA: Auditoría de autores ===';
PRINT '';

-- Mostrar auditoría actual
PRINT 'ANTES - Registros en authors_audit:';
SELECT COUNT(*) AS 'Total registros' FROM authors_audit;
PRINT '';

-- Insertar nuevo autor
PRINT 'INSERTANDO nuevo autor...';
BEGIN TRY
    INSERT INTO authors (au_id, au_lname, au_fname, phone, contract)
    VALUES ('999-99-9999', 'Prueba', 'Autor', '555-1234', 1);
    PRINT 'Autor insertado.';
END TRY
BEGIN CATCH
    PRINT 'Error: ' + ERROR_MESSAGE();
END CATCH
PRINT '';

-- Actualizar autor
PRINT 'ACTUALIZANDO autor...';
UPDATE authors SET phone = '555-9999' WHERE au_id = '999-99-9999';
PRINT 'Autor actualizado.';
PRINT '';

-- Eliminar autor
PRINT 'ELIMINANDO autor...';
DELETE FROM authors WHERE au_id = '999-99-9999';
PRINT 'Autor eliminado.';
PRINT '';

-- Mostrar auditoría después
PRINT 'DESPUÉS - Registros en authors_audit:';
SELECT audit_id, action_type, au_id, notes, action_date 
FROM authors_audit 
ORDER BY audit_id;
PRINT '';

PRINT '----------------------------------------';
GO

/* ============================================= */
/* PRUEBA 3: Historial de Precios                */
/* ============================================= */
PRINT '=== 3. PRUEBA: Historial de cambios de precio ===';
PRINT '';

-- Mostrar precio actual
PRINT 'ANTES - Precio actual de BU1032:';
SELECT title_id, title, price 
FROM titles 
WHERE title_id = 'BU1032';
PRINT '';

-- Mostrar historial actual
PRINT 'Historial de precios actual:';
SELECT COUNT(*) AS 'Registros en price_history' FROM price_history;
PRINT '';

-- Cambiar precio varias veces
PRINT 'CAMBIANDO precio (1ra vez)...';
UPDATE titles SET price = 25.99 WHERE title_id = 'BU1032';
PRINT 'Precio cambiado a $25.99';
PRINT '';

PRINT 'CAMBIANDO precio (2da vez)...';
UPDATE titles SET price = 29.99 WHERE title_id = 'BU1032';
PRINT 'Precio cambiado a $29.99';
PRINT '';

PRINT 'CAMBIANDO precio a NULL...';
UPDATE titles SET price = NULL WHERE title_id = 'BU1032';
PRINT 'Precio cambiado a NULL';
PRINT '';

PRINT 'RESTAURANDO precio original...';
UPDATE titles SET price = 19.99 WHERE title_id = 'BU1032';
PRINT 'Precio restaurado a $19.99';
PRINT '';

-- Mostrar historial completo
PRINT 'DESPUÉS - Historial completo de cambios:';
SELECT history_id, title_id, old_price, new_price, change_date 
FROM price_history 
ORDER BY history_id;
PRINT '';

-- Mostrar precio final
PRINT 'Precio final de BU1032:';
SELECT title_id, title, price 
FROM titles 
WHERE title_id = 'BU1032';
PRINT '';

PRINT '----------------------------------------';
GO

/* ============================================= */
/* PRUEBA 4: Validar Royalties                   */
/* ============================================= */
PRINT '=== 4. PRUEBA: Validar royalties ===';
PRINT '';

-- Mostrar royalties actuales para un título
PRINT 'ANTES - Royalties para PC8888:';
SELECT ta.au_id, a.au_lname, a.au_fname, ta.royaltyper
FROM titleauthor ta
JOIN authors a ON ta.au_id = a.au_id
WHERE ta.title_id = 'PC8888'
ORDER BY ta.au_ord;
PRINT '';

-- Calcular total actual
PRINT 'Total actual de royalties para PC8888:';
SELECT SUM(royaltyper) AS 'Total Royalties %'
FROM titleauthor 
WHERE title_id = 'PC8888';
PRINT '';

-- Intentar agregar royalty inválido (> 100%)
PRINT 'INTENTANDO agregar royalty inválido (150%)...';
BEGIN TRY
    INSERT INTO titleauthor (au_id, title_id, au_ord, royaltyper)
    VALUES ('724-08-9931', 'PC8888', 3, 150);
    PRINT '✗ No debería llegar aquí';
END TRY
BEGIN CATCH
    PRINT '✓ Error esperado: ' + ERROR_MESSAGE();
END CATCH
PRINT '';

-- Agregar royalty válido
PRINT 'AGREGANDO royalty válido (20%)...';
BEGIN TRY
    INSERT INTO titleauthor (au_id, title_id, au_ord, royaltyper)
    VALUES ('724-08-9931', 'PC8888', 3, 20);
    PRINT '✓ Royalty agregado exitosamente';
END TRY
BEGIN CATCH
    PRINT 'Error: ' + ERROR_MESSAGE();
END CATCH
PRINT '';

-- Mostrar royalties después
PRINT 'DESPUÉS - Royalties para PC8888:';
SELECT ta.au_id, a.au_lname, a.au_fname, ta.royaltyper
FROM titleauthor ta
JOIN authors a ON ta.au_id = a.au_id
WHERE ta.title_id = 'PC8888'
ORDER BY ta.au_ord;
PRINT '';

-- Calcular total después
PRINT 'Total final de royalties para PC8888:';
SELECT SUM(royaltyper) AS 'Total Royalties %'
FROM titleauthor 
WHERE title_id = 'PC8888';
PRINT '';

-- Limpiar
DELETE FROM titleauthor 
WHERE au_id = '724-08-9931' AND title_id = 'PC8888';
PRINT 'Datos de prueba limpiados.';
PRINT '----------------------------------------';
GO

/* ============================================= */
/* PRUEBA 5: Validar Descuentos                  */
/* ============================================= */
PRINT '=== 5. PRUEBA: Validar descuentos ===';
PRINT '';

-- Mostrar descuentos actuales
PRINT 'ANTES - Descuentos existentes:';
SELECT discounttype, stor_id, lowqty, highqty, discount 
FROM discounts;
PRINT '';

-- Intentar descuento inválido (rango incorrecto)
PRINT 'INTENTANDO descuento con rango inválido (lowqty > highqty)...';
BEGIN TRY
    INSERT INTO discounts (discounttype, stor_id, lowqty, highqty, discount)
    VALUES ('Test Inválido', '7066', 10, 5, 15.0);
    PRINT '✗ No debería llegar aquí';
END TRY
BEGIN CATCH
    PRINT '✓ Error esperado: ' + ERROR_MESSAGE();
END CATCH
PRINT '';

-- Intentar descuento inválido (porcentaje > 100)
PRINT 'INTENTANDO descuento con porcentaje inválido (> 100%)...';
BEGIN TRY
    INSERT INTO discounts (discounttype, stor_id, lowqty, highqty, discount)
    VALUES ('Test Alto', '7066', 1, 10, 150.0);
    PRINT '✗ No debería llegar aquí';
END TRY
BEGIN CATCH
    PRINT '✓ Error esperado: ' + ERROR_MESSAGE();
END CATCH
PRINT '';

-- Agregar descuento válido
PRINT 'AGREGANDO descuento válido...';
BEGIN TRY
    INSERT INTO discounts (discounttype, stor_id, lowqty, highqty, discount)
    VALUES ('Test Válido', '7066', 1, 10, 15.0);
    PRINT '✓ Descuento agregado exitosamente';
END TRY
BEGIN CATCH
    PRINT 'Error: ' + ERROR_MESSAGE();
END CATCH
PRINT '';

-- Mostrar descuentos después
PRINT 'DESPUÉS - Todos los descuentos:';
SELECT discounttype, stor_id, lowqty, highqty, discount 
FROM discounts;
PRINT '';

-- Limpiar
DELETE FROM discounts WHERE discounttype = 'Test Válido';
PRINT 'Datos de prueba limpiados.';
PRINT '----------------------------------------';
GO

/* ============================================= */
/* PRUEBA 6: Prevenir Eliminación de Publishers  */
/* ============================================= */
PRINT '=== 6. PRUEBA: Prevenir eliminación de publishers ===';
PRINT '';

-- Mostrar publishers actuales
PRINT 'ANTES - Publishers existentes:';
SELECT pub_id, pub_name, city, state 
FROM publishers 
ORDER BY pub_id;
PRINT '';

-- Contar libros por publisher
PRINT 'Libros por publisher:';
SELECT p.pub_id, p.pub_name, COUNT(t.title_id) AS 'Libros'
FROM publishers p
LEFT JOIN titles t ON p.pub_id = t.pub_id
GROUP BY p.pub_id, p.pub_name
ORDER BY p.pub_id;
PRINT '';

-- Intentar eliminar publisher con libros (debería fallar)
PRINT 'INTENTANDO eliminar publisher 1389 (tiene libros)...';
BEGIN TRY
    DELETE FROM publishers WHERE pub_id = '1389';
    PRINT '✗ No debería llegar aquí';
END TRY
BEGIN CATCH
    PRINT '✓ Error: ' + ERROR_MESSAGE();
END CATCH
PRINT '';

-- Crear publisher temporal sin libros
PRINT 'CREANDO publisher temporal sin libros...';
BEGIN TRY
    INSERT INTO publishers (pub_id, pub_name, city, state, country)
    VALUES ('TEST', 'Publisher Test', 'Test City', 'TS', 'USA');
    PRINT '✓ Publisher temporal creado';
END TRY
BEGIN CATCH
    PRINT 'Error: ' + ERROR_MESSAGE();
END CATCH
PRINT '';

-- Intentar eliminar publisher sin libros (debería funcionar)
PRINT 'INTENTANDO eliminar publisher TEST (sin libros)...';
BEGIN TRY
    DELETE FROM publishers WHERE pub_id = 'TEST';
    PRINT '✓ Publisher eliminado exitosamente';
END TRY
BEGIN CATCH
    PRINT 'Error: ' + ERROR_MESSAGE();
END CATCH
PRINT '';

-- Mostrar publishers después
PRINT 'DESPUÉS - Publishers finales:';
SELECT pub_id, pub_name, city, state 
FROM publishers 
ORDER BY pub_id;
PRINT '';

PRINT '----------------------------------------';
GO

/* ============================================= */
/* PRUEBA 7: Validar Empleados                   */
/* ============================================= */
PRINT '=== 7. PRUEBA: Validar niveles de empleados ===';
PRINT '';

-- Mostrar rango de niveles para un trabajo
PRINT 'Rango de niveles para trabajos:';
SELECT job_id, job_desc, min_lvl, max_lvl 
FROM jobs 
WHERE job_id IN (1, 5, 14)
ORDER BY job_id;
PRINT '';

-- Mostrar empleados actuales
PRINT 'Muestra de empleados actuales:';
SELECT TOP 5 emp_id, fname, lname, job_id, job_lvl 
FROM employee 
ORDER BY emp_id;
PRINT '';

-- Intentar insertar empleado con nivel inválido (muy bajo)
PRINT 'INTENTANDO insertar empleado con nivel 5 (mínimo 10 para job_id 1)...';
BEGIN TRY
    INSERT INTO employee (emp_id, fname, minit, lname, job_id, job_lvl, pub_id, hire_date)
    VALUES ('TEST001M', 'John', 'A', 'Doe', 1, 5, '0736', GETDATE());
    PRINT '✗ No debería llegar aquí';
END TRY
BEGIN CATCH
    PRINT '✓ Error esperado: ' + ERROR_MESSAGE();
END CATCH
PRINT '';

-- Intentar insertar empleado con nivel inválido (muy alto)
PRINT 'INTENTANDO insertar empleado con nivel 300 (máximo 250 para job_id 5)...';
BEGIN TRY
    INSERT INTO employee (emp_id, fname, minit, lname, job_id, job_lvl, pub_id, hire_date)
    VALUES ('TEST002M', 'Jane', 'B', 'Smith', 5, 300, '0736', GETDATE());
    PRINT '✗ No debería llegar aquí';
END TRY
BEGIN CATCH
    PRINT '✓ Error esperado: ' + ERROR_MESSAGE();
END CATCH
PRINT '';

-- Insertar empleado con nivel válido
PRINT 'INSERTANDO empleado con nivel válido (150 para job_id 5)...';
BEGIN TRY
    INSERT INTO employee (emp_id, fname, minit, lname, job_id, job_lvl, pub_id, hire_date)
    VALUES ('TEST003M', 'Robert', 'C', 'Johnson', 5, 150, '0736', GETDATE());
    PRINT '✓ Empleado insertado exitosamente';
END TRY
BEGIN CATCH
    PRINT 'Error: ' + ERROR_MESSAGE();
END CATCH
PRINT '';

-- Mostrar empleado insertado
PRINT 'DESPUÉS - Empleado insertado:';
SELECT emp_id, fname, lname, job_id, job_lvl 
FROM employee 
WHERE emp_id IN ('TEST003M');
PRINT '';