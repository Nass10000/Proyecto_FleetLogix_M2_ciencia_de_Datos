# FleetLogix - Sistema de Generación de Datos Sintéticos

## 📋 Descripción del Proyecto

FleetLogix es una empresa de transporte y logística que opera una flota de 200 vehículos realizando entregas de última milla en 5 ciudades principales de República Dominicana. Este sistema genera datos sintéticos masivos y coherentes para poblar una base de datos PostgreSQL con más de **505,000 registros** distribuidos en 6 tablas interrelacionadas.

### Objetivo

Crear una infraestructura de datos robusta que permita análisis operativos, toma de decisiones basada en datos, y simulación de operaciones logísticas reales.

---

## 🗄️ Modelo de Datos Relacional

### Tablas del Sistema

El sistema consta de **6 tablas** organizadas en dos categorías:

#### Tablas Maestras (650 registros)
1. **vehicles** - 200 vehículos de la flota
2. **drivers** - 400 conductores empleados
3. **routes** - 50 rutas entre ciudades

#### Tablas Transaccionales (505,000+ registros)
4. **trips** - 100,000 viajes realizados
5. **deliveries** - ~400,000 entregas individuales
6. **maintenance** - ~5,000 registros de mantenimiento

---

## 🔗 Relaciones y Constraints

### Diagrama de Relaciones

```
vehicles (1) ──────< (N) trips
vehicles (1) ──────< (N) maintenance
drivers (1)  ──────< (N) trips
routes (1)   ──────< (N) trips
trips (1)    ──────< (N) deliveries
```

### Relaciones Detalladas

#### 1. vehicles → trips (1:N)
- **Cardinalidad:** Un vehículo puede realizar múltiples viajes
- **Foreign Key:** `trips.vehicle_id` → `vehicles.vehicle_id`
- **Constraint:** ON DELETE RESTRICT (no se pueden eliminar vehículos con viajes)
- **Regla de Negocio:** Cada viaje requiere exactamente un vehículo

#### 2. drivers → trips (1:N)
- **Cardinalidad:** Un conductor puede realizar múltiples viajes
- **Foreign Key:** `trips.driver_id` → `drivers.driver_id`
- **Constraint:** ON DELETE RESTRICT
- **Regla de Negocio:** Cada viaje es conducido por exactamente un conductor

#### 3. routes → trips (1:N)
- **Cardinalidad:** Una ruta puede ser usada en múltiples viajes
- **Foreign Key:** `trips.route_id` → `routes.route_id`
- **Constraint:** ON DELETE RESTRICT
- **Regla de Negocio:** Cada viaje sigue exactamente una ruta predefinida

#### 4. trips → deliveries (1:N)
- **Cardinalidad:** Un viaje contiene entre 2 y 6 entregas
- **Foreign Key:** `deliveries.trip_id` → `trips.trip_id`
- **Constraint:** ON DELETE CASCADE (eliminar viaje elimina sus entregas)
- **Regla de Negocio:** Cada entrega pertenece a exactamente un viaje
- **Distribución:** 2 (10%), 3 (20%), 4 (40%), 5 (20%), 6 (10%)

#### 5. vehicles → maintenance (1:N)
- **Cardinalidad:** Un vehículo tiene múltiples registros de mantenimiento
- **Foreign Key:** `maintenance.vehicle_id` → `vehicles.vehicle_id`
- **Constraint:** ON DELETE CASCADE
- **Regla de Negocio:** Mantenimiento programado cada ~20 viajes por vehículo

---

## 🔐 Constraints del Sistema

### Primary Keys (PKs)
- `vehicles.vehicle_id` - SERIAL PRIMARY KEY
- `drivers.driver_id` - SERIAL PRIMARY KEY
- `routes.route_id` - SERIAL PRIMARY KEY
- `trips.trip_id` - SERIAL PRIMARY KEY
- `deliveries.delivery_id` - SERIAL PRIMARY KEY
- `maintenance.maintenance_id` - SERIAL PRIMARY KEY

### Unique Constraints
1. `vehicles.license_plate` - Placas únicas (formato dominicano: A123456)
2. `drivers.employee_code` - Códigos de empleado únicos (EMP-####)
3. `drivers.license_number` - Números de licencia únicos (LIC-#########)
4. `routes.route_code` - Códigos de ruta únicos (RT-###)
5. `deliveries.tracking_number` - Números de tracking únicos (DOM##########)

### Default Values
- `vehicles.status` - DEFAULT 'active'
- `drivers.status` - DEFAULT 'active'
- `routes.toll_cost` - DEFAULT 0
- `trips.status` - DEFAULT 'in_progress'
- `deliveries.delivery_status` - DEFAULT 'pending'
- `deliveries.recipient_signature` - DEFAULT FALSE

### Índices Creados
```sql
CREATE INDEX idx_trips_departure ON trips(departure_datetime);
CREATE INDEX idx_deliveries_status ON deliveries(delivery_status);
CREATE INDEX idx_vehicles_status ON vehicles(status);
```

---

## 📊 Explicación de Métodos Clave

### 1. `get_hourly_distribution()`

#### Propósito
Retorna un array de 24 probabilidades que simula el patrón operativo típico de una empresa de logística, evitando la distribución uniforme poco realista.

#### ⚠️ Nota sobre el Origen de las Probabilidades

Las probabilidades utilizadas en este método son **estimaciones basadas en patrones típicos del sector logístico**, NO datos históricos reales de FleetLogix (ya que estamos generando datos sintéticos).

En un entorno real con datos históricos, estos valores deberían calcularse mediante:
```sql
SELECT EXTRACT(HOUR FROM departure_datetime) as hora,
       COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as porcentaje_real
FROM trips_historicos
GROUP BY hora;
```

Para este proyecto sintético, se utilizaron valores razonables basados en:
- Patrones conocidos de empresas de entrega (Amazon, DHL, FedEx)
- Lógica operativa común (picos matutinos, reducción nocturna)
- Suposiciones de comportamiento logístico estándar

#### Funcionamiento

En lugar de asignar **4.17% a cada hora** (distribución uniforme), este método asigna probabilidades estimadas que simulan el comportamiento típico de operaciones logísticas:

```python
return np.array([
    0.005, 0.005, 0.005, 0.005, 0.010, 0.020,  # 00-05: Madrugada
    0.080, 0.085, 0.085, 0.075, 0.065, 0.055,  # 06-11: Pico matutino
    0.040, 0.040, 0.065, 0.070, 0.070, 0.060,  # 12-17: Almuerzo + pico vespertino
    0.045, 0.035, 0.025, 0.020, 0.015, 0.010   # 18-23: Noche
])
```

#### Distribución por Franjas Horarias

| Franja Horaria | Horas | Probabilidad/hora | Justificación |
|----------------|-------|-------------------|---------------|
| **Madrugada** | 00:00 - 05:59 | 0.5% - 2.0% | Actividad mínima, solo operaciones especiales |
| **Pico Matutino** 🔥 | 06:00 - 09:59 | 7.5% - 8.5% | **MÁXIMO DEL DÍA** - Salida principal de flota |
| **Media Mañana** | 10:00 - 11:59 | 5.5% - 6.5% | Alta actividad sostenida |
| **Almuerzo** | 12:00 - 13:59 | 4.0% | Reducción por horario de comida |
| **Pico Vespertino** 🔥 | 14:00 - 17:59 | 6.0% - 7.0% | Segundo pico operativo |
| **Noche** | 18:00 - 23:59 | 1.0% - 4.5% | Disminución progresiva |

#### Picos Operativos

**Pico Máximo:** 07:00 - 08:00 (8.5% cada hora)
- Aprovecha tráfico ligero matutino
- Entregas programadas para la mañana
- Salida masiva de la flota

**Segundo Pico:** 15:00 - 16:00 (7.0% cada hora)
- Entregas de tarde
- Completar rutas pendientes

#### Uso en `generate_trips()`

```python
# Obtener distribución horaria
hourly_probs = get_hourly_distribution()

# Seleccionar hora ponderada (NO uniforme)
selected_hour = np.random.choice(24, p=hourly_probs)
```

Esto crea un patrón realista donde:
- 7-8am tiene **17%** de todos los viajes del día
- 0-5am tiene solo **3%** de todos los viajes del día

---

### 2. `generate_trips()`

#### Propósito
Genera 100,000 viajes que representan 2 años de operación de FleetLogix (2024-2025) con coherencia total entre reglas de negocio y física del mundo real.

#### Algoritmo en 7 Pasos

### PASO 1: Selección de Fecha y Hora

```python
# Fecha uniforme entre 2024-01-01 y 2025-12-31
random_seconds = np.random.randint(0, total_seconds)
departure_datetime = start_date + timedelta(seconds=random_seconds)

# Hora NO uniforme - usa get_hourly_distribution()
selected_hour = np.random.choice(24, p=hourly_probs)
```

**Justificación:**
- Cada día del período tiene igual probabilidad
- Las horas siguen distribución realista (picos matutinos/vespertinos)
- Total: 731 días × 24 horas = 17,544 slots temporales posibles

---

### PASO 2: Asignación de Foreign Keys

```python
vehicle_id = np.random.randint(1, NUM_VEHICLES + 1)    # 1-200
driver_id = np.random.randint(1, NUM_DRIVERS + 1)      # 1-400
route_id = np.random.randint(1, len(routes_df) + 1)    # 1-50
```

**Garantías de Integridad Referencial:**
- Todos los `vehicle_id` existen en tabla `vehicles`
- Todos los `driver_id` existen en tabla `drivers`
- Todos los `route_id` existen en tabla `routes`

**Realismo:**
- 400 conductores / 200 vehículos = Turnos compartidos
- Algunos vehículos/rutas más populares que otros (distribución natural)

---

### PASO 3: Recuperación de Datos de Ruta

```python
route = routes_df.iloc[route_id - 1]
distance_km = route['distance_km']
estimated_duration_hours = route['estimated_duration_hours']
```

Se recuperan los datos reales de la ruta para cálculos posteriores.

---

### PASO 4: Cálculo de Hora de Llegada ⏰

**FÓRMULA CLAVE PARA CONSISTENCIA TEMPORAL:**

```python
# Duración real varía ±20% (tráfico, clima, etc.)
actual_duration = estimated_duration × random(0.8, 1.2)

# arrival SIEMPRE mayor que departure
arrival_datetime = departure_datetime + actual_duration
```

**Por qué funciona:**
1. `estimated_duration` es la duración base (de tabla `routes`)
2. Factor de variación (0.8 a 1.2) es **SIEMPRE POSITIVO**
3. Por lo tanto: `arrival > departure` **GARANTIZADO**

**Factor de Variación simula:**
- ⛈️ Condiciones climáticas
- 🚗 Tráfico ligero/pesado (0.8 = rápido, 1.2 = lento)
- 👤 Habilidad del conductor
- 📦 Retrasos en puntos de entrega

**Ejemplo:**
```
Ruta Santo Domingo → Santiago
Distancia: 155 km
Duración estimada: 2.5 horas

Variación posible:
- Mejor caso: 2.5 × 0.8 = 2.0 horas (tráfico fluido)
- Peor caso: 2.5 × 1.2 = 3.0 horas (tráfico pesado)
```

---

### PASO 5: Cálculo de Combustible Consumido ⛽

**FÓRMULA:**

```python
fuel_consumed = (distance_km / km_per_liter) × random(0.9, 1.1)
```

**Rendimiento por Tipo de Vehículo:**

| Tipo Vehículo | Rendimiento | Ejemplo (150 km) |
|---------------|-------------|------------------|
| Camión Grande | 3.5 km/L | 38.6 - 47.2 L |
| Camión Mediano | 5.0 km/L | 27.0 - 33.0 L |
| Van | 8.0 km/L | 16.9 - 20.6 L |
| Motocicleta | 25.0 km/L | 5.4 - 6.6 L |

**Factor de Variación (±10%) simula:**
- 🏎️ Estilo de conducción (agresivo vs económico)
- 📦 Peso de la carga (más peso = más consumo)
- 🏔️ Condiciones del terreno (montaña vs plano)
- 🔧 Estado del mantenimiento del vehículo

**Coherencia:**
- Camiones grandes consumen **7.1× más** que motocicletas
- Proporcional a la realidad del sector

---

### PASO 6: Asignación de Peso de Carga 📦

**FÓRMULA:**

```python
total_weight_kg = capacity_kg × load_factor
load_factor = random(0.5, 0.95)
```

**Justificación del Rango (50% - 95%):**

| Factor | Por qué NO menos de 50% | Por qué NO más de 95% |
|--------|-------------------------|------------------------|
| Económico | Vehículo vacío = ineficiente | Sobrecarga arriesga multas |
| Operativo | No se envían viajes sin carga significativa | Espacio para variación/imprevistos |
| Realista | Consolidación de entregas | Optimización de rutas parciales |

**Ejemplo Real:**
```
Van con capacidad 1,500 kg:
- Mínimo: 1,500 × 0.50 = 750 kg
- Promedio: 1,500 × 0.725 = 1,088 kg
- Máximo: 1,500 × 0.95 = 1,425 kg
```

**Distribución Natural:**
- Factor promedio: 72.5%
- Refleja operación real de logística de última milla

---

### PASO 7: Asignación de Estado del Viaje 📊

**Distribución:**

| Estado | Probabilidad | Regla arrival_datetime |
|--------|--------------|------------------------|
| **completed** | 95% | Siempre tiene valor |
| **in_progress** | 3% | Siempre NULL |
| **cancelled** | 2% | 50% NULL, 50% tiene valor |

```python
status = np.random.choice(
    ['completed', 'in_progress', 'cancelled'],
    p=[0.95, 0.03, 0.02]
)

# Aplicar reglas
if status == 'in_progress':
    arrival_datetime = None  # Aún no ha llegado
elif status == 'cancelled' and random() < 0.5:
    arrival_datetime = None  # Cancelado antes de completar
```

**Justificación:**
- 95% completados es realista para operaciones establecidas
- 3% en progreso representa operaciones del "día actual"
- 2% cancelados refleja problemas ocasionales (clima, averías, etc.)

---

## ✅ Garantías de Consistencia

### Integridad Referencial
✓ Todos los `vehicle_id` en trips existen en vehicles (1-200)  
✓ Todos los `driver_id` en trips existen en drivers (1-400)  
✓ Todos los `route_id` en trips existen en routes (1-50)  
✓ Todos los `trip_id` en deliveries existen en trips  
✓ Todos los `vehicle_id` en maintenance existen en vehicles  

### Consistencia Temporal
✓ `arrival_datetime > departure_datetime` SIEMPRE (cuando no NULL)  
✓ Todas las fechas dentro del rango 2024-2025  
✓ Licencias de conductores válidas durante período operativo  
✓ Fechas de mantenimiento coherentes con historial de viajes  

### Coherencia Física
✓ Consumo de combustible proporcional a distancia y tipo de vehículo  
✓ Peso cargado NUNCA excede capacidad del vehículo (50-95%)  
✓ Duración del viaje proporcional a distancia (velocidad 55-65 km/h)  
✓ Suma de pesos de entregas ≤ peso total del viaje (±1% tolerancia)  

### Reglas de Negocio
✓ Viajes "in_progress" no tienen hora de llegada  
✓ Viajes "completed" siempre tienen hora de llegada  
✓ 95% de viajes completados exitosamente  
✓ Vehículos nunca van vacíos (mínimo 50% cargados)  
✓ Entre 2-6 entregas por viaje (4 más común)  
✓ Mantenimiento cada ~20 viajes por vehículo  

---

## 📈 Rangos de Validación

### Vehicles

| Campo | Rango Válido | Validación |
|-------|--------------|------------|
| capacity_kg | 50 - 12,000 | Según tipo de vehículo |
| license_plate | Único | Formato dominicano A######|
| status | active, inactive, maintenance | - |

### Drivers

| Campo | Rango Válido | Validación |
|-------|--------------|------------|
| license_expiry | 2027-2030 | Válidas durante operación |
| employee_code | EMP-0001 a EMP-0400 | Único |
| hire_date | 2020-2025 | Coherente con operación |

### Routes

| Campo | Rango Válido | Validación |
|-------|--------------|------------|
| distance_km | 50 - 400 | Basado en matriz real |
| estimated_duration_hours | 0.5 - 8.0 | Velocidad 55-65 km/h |
| toll_cost | 0 - 20 | ~$0.50-$1.50 por 50km |

### Trips

| Campo | Rango Válido | Validación |
|-------|--------------|------------|
| fuel_consumed_liters | 1 - 200 | Según tipo y distancia |
| total_weight_kg | 50 - 12,000 | 50-95% de capacidad |
| arrival > departure | Siempre | Consistencia temporal |

### Deliveries

| Campo | Rango Válido | Validación |
|-------|--------------|------------|
| package_weight_kg | > 0 | Suma ≤ trip weight |
| tracking_number | DOM############ | Único |
| scheduled_datetime | Durante viaje | Entre departure y arrival |

### Maintenance

| Campo | Rango Válido | Validación |
|-------|--------------|------------|
| cost | 50 - 800 | Según tipo |
| next_maintenance_date | 75-105 días después | > maintenance_date |

---

## 🚀 Uso del Sistema

### Requisitos Previos

1. **PostgreSQL 15+** instalado y corriendo
2. **Python 3.10+**
3. **Librerías Python:**
   ```bash
   pip install -r requirements.txt
   ```

### Pasos de Ejecución

#### 1. Crear Base de Datos y Tablas

```bash
# Opción A: Usando psql
psql -U postgres -d fleetlogix -f fleetlogix_db_schema.sql

# Opción B: Usando pgAdmin
# Abrir Query Tool y ejecutar el contenido del archivo SQL
```

#### 2. Configurar Credenciales

Crear archivo `.env` en la raíz del proyecto basándose en `.env.example`:
```bash
cp .env.example .env
```

Luego editar `.env` con tus credenciales reales de PostgreSQL:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=fleetlogix
DB_USER=postgres
DB_PASSWORD=tu_contraseña_aqui
```

#### 3. Ejecutar Generador

```bash
python fleetlogix_generator.py
```

#### 4. Proceso Automático

El sistema ejecutará automáticamente:
1. Verificación de tablas existentes
2. Limpieza de datos anteriores
3. Generación de 200 vehículos
4. Generación de 400 conductores
5. Generación de 50 rutas
6. Generación de 100,000 viajes (1-2 minutos)
7. Generación de ~400,000 entregas (1-2 minutos)
8. Generación de ~5,000 mantenimientos
9. Carga a PostgreSQL (por lotes de 1000)
10. Validación exhaustiva (7 categorías)

**Tiempo estimado total:** 3-5 minutos

---

## 📊 Estadísticas Esperadas

### Registros Totales
```
vehicles:       200
drivers:        400
routes:          50
trips:      100,000
deliveries: ~400,000
maintenance: ~5,000
─────────────────────
TOTAL:      ~505,650
```

### Distribución de Estados

**Viajes:**
- Completados: ~95,000 (95%)
- En progreso: ~3,000 (3%)
- Cancelados: ~2,000 (2%)

**Entregas:**
- Entregadas: ~340,000 (85%)
- Pendientes: ~40,000 (10%)
- Fallidas: ~20,000 (5%)

**Entregas por Viaje:**
- 2 entregas: 10,000 viajes (10%)
- 3 entregas: 20,000 viajes (20%)
- 4 entregas: 40,000 viajes (40%) ⭐
- 5 entregas: 20,000 viajes (20%)
- 6 entregas: 10,000 viajes (10%)

---

## 🔍 Validaciones Implementadas

El sistema realiza 7 categorías de validación automática:

### 1. Conteos de Registros
Verifica que todas las tablas tengan el número esperado de registros.

### 2. Integridad Referencial
Valida que todas las foreign keys apunten a registros existentes:
- ✓ trips → vehicles
- ✓ trips → drivers
- ✓ trips → routes
- ✓ deliveries → trips
- ✓ maintenance → vehicles

### 3. Consistencia Temporal
Verifica que:
- ✓ `arrival_datetime > departure_datetime` (cuando no NULL)
- ✓ No hay fechas futuras
- ✓ Licencias válidas durante período operativo

### 4. Constraints Únicos
Valida que no haya duplicados en:
- ✓ vehicles.license_plate
- ✓ drivers.employee_code
- ✓ drivers.license_number
- ✓ deliveries.tracking_number

### 5. Coherencia de Pesos
Verifica que:
- ✓ Suma de pesos de entregas ≤ peso total del viaje (±1%)

### 6. Rangos Lógicos
Valida que los valores estén en rangos realistas:
- ✓ Capacidades de vehículos (50-15,000 kg)
- ✓ Consumo de combustible (1-1,000 L)
- ✓ Distancias de rutas (0-500 km)
- ✓ Pesos de viajes (0-15,000 kg)

### 7. Fechas Válidas
Verifica coherencia de fechas:
- ✓ Licencias no expiradas antes de 2024
- ✓ maintenance_date < next_maintenance_date

---

## 📁 Estructura de Archivos

```
Parte1-PI/
├── .env                        # Credenciales de base de datos
├── fleetlogix_generator.py    # Script principal (TODO EN UNO)
├── requirements.txt            # Dependencias Python
├── README.md                   # Esta documentación
└── fleetlogix_db_schema.sql   # Script SQL de creación de tablas
```

---

## 🛠️ Tecnologías Utilizadas

- **Base de Datos:** PostgreSQL 15+
- **Lenguaje:** Python 3.10+
- **Librerías:**
  - `psycopg2-binary` - Conector PostgreSQL
  - `pandas` - Manipulación de DataFrames
  - `numpy` - Operaciones numéricas y aleatorias
  - `faker` - Generación de datos sintéticos
  - `python-dotenv` - Gestión de variables de entorno
  - `tabulate` - Formateo de tablas en consola

---

## 👤 Autor

**Sistema FleetLogix**  
Proyecto Integrador - Módulo 2  
Ciencia de Datos  

---

## 📝 Notas Adicionales

### Reproducibilidad
El sistema usa `RANDOM_SEED = 42` para garantizar que los mismos datos se generen en cada ejecución. Para generar datos diferentes, cambiar el valor de la semilla en la configuración.

### Personalización
Para modificar la cantidad de registros, editar las constantes en el archivo:
```python
NUM_VEHICLES = 200
NUM_DRIVERS = 400
NUM_ROUTES = 50
NUM_TRIPS = 100000
```

### Rendimiento
- Generación: ~2-3 minutos
- Carga a DB: ~1-2 minutos
- Validación: ~30 segundos
- **Total: 3-5 minutos**

Para mejorar rendimiento:
- Aumentar `batch_size` en `load_data_to_table()` (default: 1000)
- Deshabilitar validaciones durante desarrollo
- Usar PostgreSQL en SSD

---

## ⚠️ Advertencias

1. **NO ejecutar en base de datos de producción** - El script hace TRUNCATE de todas las tablas
2. **Backup recomendado** antes de ejecutar si ya hay datos importantes
3. **Memoria RAM:** Se recomienda mínimo 4GB disponibles para procesar 505k registros
4. **Espacio en disco:** ~500MB para la base de datos completa

---

## 📧 Soporte

Para problemas o preguntas sobre el sistema, contactar al equipo de desarrollo de FleetLogix.

---

**Última actualización:** Enero 2026
