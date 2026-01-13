-- ═══════════════════════════════════════════════════════════════════════════════
-- FLEETLOGIX - Schema Completo con TODOS los Constraints
-- Base de datos PostgreSQL
-- Sistema de gestión de transporte y logística - 505,650 registros sintéticos
-- ═══════════════════════════════════════════════════════════════════════════════

-- ───────────────────────────────────────────────────────────────────────────────
-- ELIMINAR TABLAS SI EXISTEN (para re-creación limpia)
-- ───────────────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS maintenance CASCADE;
DROP TABLE IF EXISTS deliveries CASCADE;
DROP TABLE IF EXISTS trips CASCADE;
DROP TABLE IF EXISTS routes CASCADE;
DROP TABLE IF EXISTS drivers CASCADE;
DROP TABLE IF EXISTS vehicles CASCADE;

-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLAS MAESTRAS
-- ═══════════════════════════════════════════════════════════════════════════════

-- ───────────────────────────────────────────────────────────────────────────────
-- TABLA: vehicles (Vehículos de la flota - 200 registros)
-- ───────────────────────────────────────────────────────────────────────────────

CREATE TABLE vehicles (
    vehicle_id SERIAL PRIMARY KEY,
    license_plate VARCHAR(20) NOT NULL UNIQUE,
    vehicle_type VARCHAR(50) NOT NULL,
    capacity_kg DECIMAL(10,2),
    fuel_type VARCHAR(20),
    acquisition_date DATE,
    status VARCHAR(20) DEFAULT 'active',
    
    -- CHECK Constraints
    CONSTRAINT chk_vehicles_capacity_positive 
        CHECK (capacity_kg > 0 AND capacity_kg <= 15000),
    CONSTRAINT chk_vehicles_status_valid 
        CHECK (status IN ('active', 'inactive', 'maintenance'))
);

COMMENT ON TABLE vehicles IS 'Vehículos de la flota de FleetLogix';
COMMENT ON COLUMN vehicles.license_plate IS 'Placa única del vehículo (formato: A123456)';
COMMENT ON COLUMN vehicles.capacity_kg IS 'Capacidad de carga en kilogramos (50-15,000)';
COMMENT ON COLUMN vehicles.status IS 'Estado del vehículo: active, inactive, maintenance';

-- ───────────────────────────────────────────────────────────────────────────────
-- TABLA: drivers (Conductores empleados - 400 registros)
-- ───────────────────────────────────────────────────────────────────────────────

CREATE TABLE drivers (
    driver_id SERIAL PRIMARY KEY,
    employee_code VARCHAR(20) NOT NULL UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    license_number VARCHAR(50) NOT NULL UNIQUE,
    license_expiry DATE,
    phone VARCHAR(20),
    hire_date DATE,
    status VARCHAR(20) DEFAULT 'active',
    
    -- CHECK Constraints
    CONSTRAINT chk_drivers_license_expiry 
        CHECK (license_expiry IS NULL OR license_expiry >= hire_date),
    CONSTRAINT chk_drivers_status_valid 
        CHECK (status IN ('active', 'inactive', 'on_leave'))
);

COMMENT ON TABLE drivers IS 'Información de conductores empleados';
COMMENT ON COLUMN drivers.employee_code IS 'Código único de empleado (formato: EMP-0001)';
COMMENT ON COLUMN drivers.license_number IS 'Número de licencia de conducir (formato: LIC-123456789)';
COMMENT ON COLUMN drivers.status IS 'Estado del conductor: active, inactive, on_leave';

-- ───────────────────────────────────────────────────────────────────────────────
-- TABLA: routes (Rutas predefinidas - 50 registros)
-- ───────────────────────────────────────────────────────────────────────────────

CREATE TABLE routes (
    route_id SERIAL PRIMARY KEY,
    route_code VARCHAR(20) NOT NULL UNIQUE,
    origin_city VARCHAR(100) NOT NULL,
    destination_city VARCHAR(100) NOT NULL,
    distance_km DECIMAL(10,2),
    estimated_duration_hours DECIMAL(5,2),
    toll_cost DECIMAL(10,2) DEFAULT 0,
    
    -- CHECK Constraints
    CONSTRAINT chk_routes_distance_positive 
        CHECK (distance_km > 0 AND distance_km <= 500),
    CONSTRAINT chk_routes_duration_positive 
        CHECK (estimated_duration_hours > 0),
    CONSTRAINT chk_routes_toll_nonnegative 
        CHECK (toll_cost >= 0)
);

COMMENT ON TABLE routes IS 'Rutas predefinidas entre ciudades principales';
COMMENT ON COLUMN routes.route_code IS 'Código único de ruta (formato: RT-001)';
COMMENT ON COLUMN routes.distance_km IS 'Distancia de la ruta en kilómetros (50-500)';

-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLAS TRANSACCIONALES
-- ═══════════════════════════════════════════════════════════════════════════════

-- ───────────────────────────────────────────────────────────────────────────────
-- TABLA: trips (Viajes realizados - 100,000 registros)
-- ───────────────────────────────────────────────────────────────────────────────

CREATE TABLE trips (
    trip_id SERIAL PRIMARY KEY,
    vehicle_id INTEGER NOT NULL,
    driver_id INTEGER NOT NULL,
    route_id INTEGER NOT NULL,
    departure_datetime TIMESTAMP NOT NULL,
    arrival_datetime TIMESTAMP,
    fuel_consumed_liters DECIMAL(10,2),
    total_weight_kg DECIMAL(10,2),
    status VARCHAR(20) DEFAULT 'in_progress',
    
    -- FOREIGN KEYS
    CONSTRAINT fk_trips_vehicle 
        FOREIGN KEY (vehicle_id) REFERENCES vehicles(vehicle_id) 
        ON DELETE RESTRICT,
    CONSTRAINT fk_trips_driver 
        FOREIGN KEY (driver_id) REFERENCES drivers(driver_id) 
        ON DELETE RESTRICT,
    CONSTRAINT fk_trips_route 
        FOREIGN KEY (route_id) REFERENCES routes(route_id) 
        ON DELETE RESTRICT,
    
    -- CHECK Constraints
    CONSTRAINT chk_trips_arrival_after_departure 
        CHECK (arrival_datetime IS NULL OR arrival_datetime > departure_datetime),
    CONSTRAINT chk_trips_fuel_positive 
        CHECK (fuel_consumed_liters IS NULL OR fuel_consumed_liters > 0),
    CONSTRAINT chk_trips_weight_positive 
        CHECK (total_weight_kg IS NULL OR total_weight_kg > 0),
    CONSTRAINT chk_trips_status_valid 
        CHECK (status IN ('completed', 'in_progress', 'cancelled'))
);

COMMENT ON TABLE trips IS 'Registro de viajes realizados (2 años: 2024-2025)';
COMMENT ON COLUMN trips.status IS 'Estado: completed (95%), in_progress (3%), cancelled (2%)';

-- ───────────────────────────────────────────────────────────────────────────────
-- TABLA: deliveries (Entregas individuales - 400,000 registros)
-- ───────────────────────────────────────────────────────────────────────────────

CREATE TABLE deliveries (
    delivery_id SERIAL PRIMARY KEY,
    trip_id INTEGER NOT NULL,
    tracking_number VARCHAR(50) NOT NULL UNIQUE,
    customer_name VARCHAR(200) NOT NULL,
    delivery_address TEXT NOT NULL,
    package_weight_kg DECIMAL(10,2),
    scheduled_datetime TIMESTAMP,
    delivered_datetime TIMESTAMP,
    delivery_status VARCHAR(20) DEFAULT 'pending',
    recipient_signature BOOLEAN DEFAULT FALSE,
    
    -- FOREIGN KEY
    CONSTRAINT fk_deliveries_trip 
        FOREIGN KEY (trip_id) REFERENCES trips(trip_id) 
        ON DELETE CASCADE,
    
    -- CHECK Constraints
    CONSTRAINT chk_deliveries_weight_positive 
        CHECK (package_weight_kg IS NULL OR package_weight_kg > 0),
    CONSTRAINT chk_deliveries_datetime_order 
        CHECK (delivered_datetime IS NULL OR scheduled_datetime IS NULL 
               OR delivered_datetime >= scheduled_datetime),
    CONSTRAINT chk_deliveries_status_valid 
        CHECK (delivery_status IN ('delivered', 'pending', 'failed'))
);

COMMENT ON TABLE deliveries IS 'Entregas individuales asociadas a cada viaje (2-6 por viaje)';
COMMENT ON COLUMN deliveries.tracking_number IS 'Número de rastreo único (formato: DOM1234567890)';
COMMENT ON COLUMN deliveries.delivery_status IS 'Estado: delivered (81.7%), pending (12.8%), failed (5.5%)';

-- ───────────────────────────────────────────────────────────────────────────────
-- TABLA: maintenance (Mantenimientos de vehículos - 5,000 registros)
-- ───────────────────────────────────────────────────────────────────────────────

CREATE TABLE maintenance (
    maintenance_id SERIAL PRIMARY KEY,
    vehicle_id INTEGER NOT NULL,
    maintenance_date DATE NOT NULL,
    maintenance_type VARCHAR(50) NOT NULL,
    description TEXT,
    cost DECIMAL(10,2),
    next_maintenance_date DATE,
    performed_by VARCHAR(200),
    
    -- FOREIGN KEY
    CONSTRAINT fk_maintenance_vehicle 
        FOREIGN KEY (vehicle_id) REFERENCES vehicles(vehicle_id) 
        ON DELETE CASCADE,
    
    -- CHECK Constraints
    CONSTRAINT chk_maintenance_next_date_after 
        CHECK (next_maintenance_date IS NULL OR next_maintenance_date > maintenance_date),
    CONSTRAINT chk_maintenance_cost_positive 
        CHECK (cost IS NULL OR cost > 0)
);

COMMENT ON TABLE maintenance IS 'Historial de mantenimiento de vehículos (~1 cada 20 viajes)';
COMMENT ON COLUMN maintenance.maintenance_type IS 'Tipo: Cambio de aceite, Revisión de frenos, Cambio de llantas, etc.';

-- ═══════════════════════════════════════════════════════════════════════════════
-- ÍNDICES PERSONALIZADOS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE INDEX idx_trips_departure ON trips(departure_datetime);
CREATE INDEX idx_deliveries_status ON deliveries(delivery_status);
CREATE INDEX idx_vehicles_status ON vehicles(status);

COMMENT ON INDEX idx_trips_departure IS 'Índice para búsquedas por fecha de salida';
COMMENT ON INDEX idx_deliveries_status IS 'Índice para filtros por estado de entrega';
COMMENT ON INDEX idx_vehicles_status IS 'Índice para filtros por estado de vehículo';

-- ═══════════════════════════════════════════════════════════════════════════════
-- RESUMEN DE CONSTRAINTS IMPLEMENTADOS
-- ═══════════════════════════════════════════════════════════════════════════════

/*
PRIMARY KEYS (6):
  ✓ vehicles.vehicle_id           SERIAL PRIMARY KEY
  ✓ drivers.driver_id             SERIAL PRIMARY KEY
  ✓ routes.route_id               SERIAL PRIMARY KEY
  ✓ trips.trip_id                 SERIAL PRIMARY KEY
  ✓ deliveries.delivery_id        SERIAL PRIMARY KEY
  ✓ maintenance.maintenance_id    SERIAL PRIMARY KEY

UNIQUE CONSTRAINTS (5):
  ✓ vehicles.license_plate        UNIQUE (formato: A123456)
  ✓ drivers.employee_code         UNIQUE (formato: EMP-0001 a EMP-0400)
  ✓ drivers.license_number        UNIQUE (formato: LIC-123456789)
  ✓ routes.route_code             UNIQUE (formato: RT-001 a RT-050)
  ✓ deliveries.tracking_number    UNIQUE (formato: DOM1234567890)

FOREIGN KEYS (5):
  ✓ trips.vehicle_id    → vehicles.vehicle_id   (ON DELETE RESTRICT)
  ✓ trips.driver_id     → drivers.driver_id     (ON DELETE RESTRICT)
  ✓ trips.route_id      → routes.route_id       (ON DELETE RESTRICT)
  ✓ deliveries.trip_id  → trips.trip_id         (ON DELETE CASCADE)
  ✓ maintenance.vehicle_id → vehicles.vehicle_id (ON DELETE CASCADE)

DEFAULT VALUES (6):
  ✓ vehicles.status              DEFAULT 'active'
  ✓ drivers.status               DEFAULT 'active'
  ✓ routes.toll_cost             DEFAULT 0
  ✓ trips.status                 DEFAULT 'in_progress'
  ✓ deliveries.delivery_status   DEFAULT 'pending'
  ✓ deliveries.recipient_signature DEFAULT FALSE

CHECK CONSTRAINTS (16):
  ✓ vehicles.capacity_kg         > 0 AND <= 15000
  ✓ vehicles.status              IN ('active', 'inactive', 'maintenance')
  ✓ drivers.license_expiry       >= hire_date
  ✓ drivers.status               IN ('active', 'inactive', 'on_leave')
  ✓ routes.distance_km           > 0 AND <= 500
  ✓ routes.estimated_duration    > 0
  ✓ routes.toll_cost             >= 0
  ✓ trips.arrival_datetime       > departure_datetime (cuando NOT NULL)
  ✓ trips.fuel_consumed_liters   > 0
  ✓ trips.total_weight_kg        > 0
  ✓ trips.status                 IN ('completed', 'in_progress', 'cancelled')
  ✓ deliveries.package_weight_kg > 0
  ✓ deliveries.datetime_order    delivered >= scheduled
  ✓ deliveries.delivery_status   IN ('delivered', 'pending', 'failed')
  ✓ maintenance.next_date        > maintenance_date
  ✓ maintenance.cost             > 0

ÍNDICES (3):
  ✓ idx_trips_departure          ON trips(departure_datetime)
  ✓ idx_deliveries_status        ON deliveries(delivery_status)
  ✓ idx_vehicles_status          ON vehicles(status)
*/

-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICACIÓN DE SCHEMA CREADO
-- ═══════════════════════════════════════════════════════════════════════════════

-- Verificar tablas creadas
SELECT 
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns 
     WHERE table_schema = 'public' AND table_name = t.table_name) as columns
FROM information_schema.tables t
WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
ORDER BY table_name;

-- Verificar FOREIGN KEYS
SELECT
    tc.table_name AS tabla_origen,
    kcu.column_name AS columna,
    ccu.table_name AS tabla_destino,
    ccu.column_name AS columna_destino,
    tc.constraint_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public';

-- Verificar CHECK constraints
SELECT 
    tc.table_name,
    tc.constraint_name,
    cc.check_clause
FROM information_schema.table_constraints tc
JOIN information_schema.check_constraints cc
    ON tc.constraint_name = cc.constraint_name
WHERE tc.constraint_type = 'CHECK'
AND tc.table_schema = 'public'
AND tc.constraint_name LIKE 'chk_%'
ORDER BY tc.table_name;
