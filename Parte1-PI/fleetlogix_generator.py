"""
═══════════════════════════════════════════════════════════════════════════════
FLEETLOGIX - Sistema de Generación de Datos Sintéticos para Logística
═══════════════════════════════════════════════════════════════════════════════
Proyecto Integrador - Módulo 2
Genera 505,000+ registros sintéticos coherentes para 6 tablas interrelacionadas

Base de Datos: PostgreSQL
Tablas: vehicles, drivers, routes, trips, deliveries, maintenance
═══════════════════════════════════════════════════════════════════════════════
"""

import os
import psycopg2
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tabulate import tabulate
import sys

# ═══════════════════════════════════════════════════════════════════════════════
# SECCIÓN 1: CONFIGURACIÓN DEL SISTEMA
# ═══════════════════════════════════════════════════════════════════════════════

# Cargar variables de entorno desde archivo .env
load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# 1.1 Configuración de Base de Datos PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'fleetlogix'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}

# ─────────────────────────────────────────────────────────────────────────────
# 1.2 Semilla para Reproducibilidad
# ─────────────────────────────────────────────────────────────────────────────
RANDOM_SEED = 42

# ─────────────────────────────────────────────────────────────────────────────
# 1.3 Cantidad de Registros a Generar
# ─────────────────────────────────────────────────────────────────────────────
NUM_VEHICLES = 200      # Vehículos de la flota
NUM_DRIVERS = 400       # Conductores empleados
NUM_ROUTES = 50         # Rutas entre ciudades
NUM_TRIPS = 100000      # Viajes en 2 años (2024-2025)
NUM_DELIVERIES = 400000 # Entregas (2-6 por viaje, 4 más probable)
NUM_MAINTENANCE = 5000  # Registros de mantenimiento (~1 cada 20 viajes)

# ─────────────────────────────────────────────────────────────────────────────
# 1.4 Matriz de Distancias Reales - República Dominicana (km)
# ─────────────────────────────────────────────────────────────────────────────
CITY_DISTANCES = {
    'Santo Domingo': {
        'Santo Domingo': 0,
        'Santiago de los Caballeros': 155,
        'La Romana': 115,
        'Puerto Plata': 215,
        'Punta Cana': 180
    },
    'Santiago de los Caballeros': {
        'Santo Domingo': 155,
        'Santiago de los Caballeros': 0,
        'La Romana': 230,
        'Puerto Plata': 65,
        'Punta Cana': 285
    },
    'La Romana': {
        'Santo Domingo': 115,
        'Santiago de los Caballeros': 230,
        'La Romana': 0,
        'Puerto Plata': 295,
        'Punta Cana': 65
    },
    'Puerto Plata': {
        'Santo Domingo': 215,
        'Santiago de los Caballeros': 65,
        'La Romana': 295,
        'Puerto Plata': 0,
        'Punta Cana': 350
    },
    'Punta Cana': {
        'Santo Domingo': 180,
        'Santiago de los Caballeros': 285,
        'La Romana': 65,
        'Puerto Plata': 350,
        'Punta Cana': 0
    }
}

CITIES = list(CITY_DISTANCES.keys())

# ─────────────────────────────────────────────────────────────────────────────
# 1.5 Tipos de Vehículos y sus Características
# ─────────────────────────────────────────────────────────────────────────────
VEHICLE_TYPES = {
    'Camión Grande': {
        'capacity_range': (8000, 12000),    # Capacidad en kg
        'km_per_liter': 3.5,                # Rendimiento de combustible
        'count': 60                          # Cantidad en la flota
    },
    'Camión Mediano': {
        'capacity_range': (4000, 8000),
        'km_per_liter': 5.0,
        'count': 70
    },
    'Van': {
        'capacity_range': (1000, 2000),
        'km_per_liter': 8.0,
        'count': 50
    },
    'Motocicleta': {
        'capacity_range': (50, 150),
        'km_per_liter': 25.0,
        'count': 20
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# 1.6 Tipos de Mantenimiento con Probabilidades y Costos (USD)
# ─────────────────────────────────────────────────────────────────────────────
MAINTENANCE_TYPES = {
    'Cambio de aceite': {'probability': 0.30, 'cost_range': (50, 150)},
    'Revisión de frenos': {'probability': 0.15, 'cost_range': (100, 300)},
    'Cambio de llantas': {'probability': 0.10, 'cost_range': (200, 800)},
    'Mantenimiento general': {'probability': 0.20, 'cost_range': (150, 500)},
    'Revisión de motor': {'probability': 0.10, 'cost_range': (200, 600)},
    'Alineación y balanceo': {'probability': 0.15, 'cost_range': (50, 120)}
}

# ─────────────────────────────────────────────────────────────────────────────
# 1.7 Proveedores de Mantenimiento
# ─────────────────────────────────────────────────────────────────────────────
MAINTENANCE_PROVIDERS = [
    'Taller Central Santo Domingo',
    'Mecánica Rápida Santiago',
    'AutoService La Romana',
    'Taller FleetLogix Interno',
    'Servicio Express Puerto Plata',
    'Mantenimiento 24/7 Punta Cana'
]

# ═══════════════════════════════════════════════════════════════════════════════
# SECCIÓN 2: INICIALIZACIÓN DE GENERADORES
# ═══════════════════════════════════════════════════════════════════════════════

# Configurar Faker para nombres en español y numpy para reproducibilidad
fake = Faker('es_MX')
Faker.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)


# ═══════════════════════════════════════════════════════════════════════════════
# SECCIÓN 3: FUNCIONES DE CONEXIÓN Y GESTIÓN DE BASE DE DATOS
# ═══════════════════════════════════════════════════════════════════════════════

def get_connection():
    """
    Establece conexión con PostgreSQL usando configuración de .env
    
    Returns:
        psycopg2.connection: Objeto de conexión activa
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"❌ ERROR: No se pudo conectar a la base de datos")
        print(f"   Detalle: {e}")
        print(f"   Verifique que PostgreSQL esté corriendo y las credenciales en .env sean correctas")
        sys.exit(1)


def verify_tables():
    """
    Verifica que todas las 6 tablas requeridas existan en la base de datos
    
    Returns:
        bool: True si todas las tablas existen, False en caso contrario
    """
    required_tables = ['vehicles', 'drivers', 'routes', 'trips', 'deliveries', 'maintenance']
    
    print("🔍 Verificando existencia de tablas en la base de datos...")
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
        """)
        
        existing_tables = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        missing_tables = [t for t in required_tables if t not in existing_tables]
        
        if missing_tables:
            print(f"❌ FALTAN TABLAS: {', '.join(missing_tables)}")
            print(f"   Por favor ejecute el script SQL 'fleetlogix_db_schema.sql' primero")
            return False
        
        print(f"✓ Todas las {len(required_tables)} tablas requeridas existen")
        for table in required_tables:
            print(f"   ✓ {table}")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR al verificar tablas: {e}")
        return False


def truncate_tables():
    """
    Limpia todas las tablas en orden correcto respetando foreign keys
    Orden: deliveries → maintenance → trips → routes → drivers → vehicles
    """
    tables_order = ['deliveries', 'maintenance', 'trips', 'routes', 'drivers', 'vehicles']
    
    print("\n🗑️  Limpiando tablas existentes...")
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Verificar si hay datos antes de limpiar
        cursor.execute("SELECT COUNT(*) FROM deliveries")
        total_records = cursor.fetchone()[0]
        
        if total_records == 0:
            print("   ℹ️  Las tablas ya están vacías, omitiendo limpieza...")
            cursor.close()
            conn.close()
            return
        
        print(f"   Registros actuales: {total_records:,} en deliveries")
        
        # Usar TRUNCATE CASCADE desde la tabla padre más alta para máxima velocidad
        cursor.execute("TRUNCATE TABLE vehicles RESTART IDENTITY CASCADE")
        print(f"   ✓ Todas las tablas limpiadas con CASCADE")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✓ Todas las tablas han sido limpiadas correctamente\n")
        
    except Exception as e:
        print(f"❌ ERROR al limpiar tablas: {e}")
        sys.exit(1)


def load_data_to_table(df, table_name, batch_size=1000):
    """
    Carga un DataFrame de pandas a una tabla de PostgreSQL usando inserciones por lotes
    
    Args:
        df (pd.DataFrame): DataFrame con los datos a cargar
        table_name (str): Nombre de la tabla destino
        batch_size (int): Tamaño de lote para inserciones (default 1000)
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Reemplazar NaN y NaT con None para PostgreSQL
        df_clean = df.copy()
        df_clean = df_clean.where(pd.notnull(df_clean), None)
        
        # Obtener nombres de columnas del DataFrame
        columns = df_clean.columns.tolist()
        placeholders = ','.join(['%s'] * len(columns))
        columns_str = ','.join(columns)
        
        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        # Insertar en lotes para eficiencia
        total_rows = len(df_clean)
        for i in range(0, total_rows, batch_size):
            batch = df_clean.iloc[i:i+batch_size]
            # Convertir cada fila a tupla, manejando NaN/NaT como None
            data = []
            for _, row in batch.iterrows():
                row_data = tuple(None if pd.isna(val) else val for val in row)
                data.append(row_data)
            cursor.executemany(insert_query, data)
            
            # Mostrar progreso
            progress = min(i + batch_size, total_rows)
            print(f"   Cargando {table_name}: {progress}/{total_rows} registros", end='\r')
        
        conn.commit()
        print(f"   ✓ {table_name}: {total_rows:,} registros cargados exitosamente" + " " * 20)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\n❌ ERROR al cargar datos en {table_name}: {e}")
        sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════════════
# SECCIÓN 4: GENERACIÓN DE TABLAS MAESTRAS (VEHICLES, DRIVERS, ROUTES)
# ═══════════════════════════════════════════════════════════════════════════════

def generate_vehicles():
    """
    Genera 200 vehículos distribuidos en 4 tipos con características realistas
    
    Distribución:
    - Camión Grande: 60 unidades (capacidad 8000-12000 kg)
    - Camión Mediano: 70 unidades (capacidad 4000-8000 kg)
    - Van: 50 unidades (capacidad 1000-2000 kg)
    - Motocicleta: 20 unidades (capacidad 50-150 kg)
    
    Returns:
        pd.DataFrame: DataFrame con 200 vehículos
    """
    print("📦 Generando vehículos de la flota...")
    
    vehicles_data = []
    
    for vehicle_type, specs in VEHICLE_TYPES.items():
        count = specs['count']
        cap_min, cap_max = specs['capacity_range']
        
        for _ in range(count):
            # Generar placa dominicana única (formato: A123456)
            license_plate = f"{np.random.choice(list('ABCDEFGHJKLMNPQRSTUVWXYZ'))}{np.random.randint(100000, 999999)}"
            
            # Capacidad dentro del rango específico del tipo de vehículo
            capacity = round(np.random.uniform(cap_min, cap_max), 2)
            
            # Fecha de adquisición entre 2018 y 2024
            acquisition_date = fake.date_between(start_date='-8y', end_date='-1y')
            
            # Estado del vehículo: 90% activo, 5% inactivo, 5% en mantenimiento
            status = np.random.choice(
                ['active', 'inactive', 'maintenance'],
                p=[0.90, 0.05, 0.05]
            )
            
            # Tipo de combustible: Diesel para camiones (80%), Gasolina para el resto
            if vehicle_type == 'Motocicleta':
                fuel_type = 'Gasolina'
            else:
                fuel_type = np.random.choice(['Diesel', 'Gasolina'], p=[0.8, 0.2])
            
            vehicles_data.append({
                'license_plate': license_plate,
                'vehicle_type': vehicle_type,
                'capacity_kg': capacity,
                'fuel_type': fuel_type,
                'acquisition_date': acquisition_date,
                'status': status
            })
    
    df = pd.DataFrame(vehicles_data)
    print(f"   ✓ {len(df)} vehículos generados")
    print(f"   Distribución: Camión Grande={VEHICLE_TYPES['Camión Grande']['count']}, "
          f"Camión Mediano={VEHICLE_TYPES['Camión Mediano']['count']}, "
          f"Van={VEHICLE_TYPES['Van']['count']}, Motocicleta={VEHICLE_TYPES['Motocicleta']['count']}")
    
    return df


def generate_drivers():
    """
    Genera 400 conductores con información completa y licencias válidas
    
    Características:
    - Códigos de empleado únicos (EMP-0001 a EMP-0400)
    - Nombres y apellidos realistas en español
    - Números de licencia únicos
    - Todas las licencias válidas hasta 2027-2030
    - Fechas de contratación coherentes (2020-2025)
    
    Returns:
        pd.DataFrame: DataFrame con 400 conductores
    """
    print("\n👥 Generando conductores...")
    
    drivers_data = []
    
    for i in range(NUM_DRIVERS):
        # Código de empleado único con formato EMP-####
        employee_code = f"EMP-{i+1:04d}"
        
        # Nombre y apellido en español
        first_name = fake.first_name()
        last_name = fake.last_name()
        
        # Número de licencia único con 9 dígitos
        license_number = f"LIC-{np.random.randint(100000000, 999999999)}"
        
        # Fecha de expiración de licencia (2027-2030, siempre válida)
        license_expiry = fake.date_between(start_date='+1y', end_date='+4y')
        
        # Teléfono dominicano (formato: +1-809-XXX-XXXX)
        phone = f"+1-809-{np.random.randint(200, 999)}-{np.random.randint(1000, 9999)}"
        
        # Fecha de contratación entre 2020 y 2025
        hire_date = fake.date_between(start_date='-6y', end_date='-1m')
        
        # Estado: 92% activo, 5% inactivo, 3% en licencia
        status = np.random.choice(
            ['active', 'inactive', 'on_leave'],
            p=[0.92, 0.05, 0.03]
        )
        
        drivers_data.append({
            'employee_code': employee_code,
            'first_name': first_name,
            'last_name': last_name,
            'license_number': license_number,
            'license_expiry': license_expiry,
            'phone': phone,
            'hire_date': hire_date,
            'status': status
        })
    
    df = pd.DataFrame(drivers_data)
    print(f"   ✓ {len(df)} conductores generados")
    print(f"   Todos con licencias válidas hasta 2027-2030")
    
    return df


def generate_routes():
    """
    Genera 50 rutas entre las 5 ciudades principales de República Dominicana
    
    Ciudades: Santo Domingo, Santiago, La Romana, Puerto Plata, Punta Cana
    
    Usa matriz de distancias reales con variación ±10% para rutas alternativas
    Calcula duración estimada basada en velocidad promedio de 55-65 km/h
    Calcula costos de peajes proporcionales a distancia (~$0.50-$1.50 por 50km)
    
    Returns:
        pd.DataFrame: DataFrame con 50 rutas
    """
    print("\n🛣️  Generando rutas entre ciudades...")
    
    routes_data = []
    route_id = 1
    
    # Con 5 ciudades tenemos 20 combinaciones únicas (origen != destino)
    # Para llegar a 50 rutas, generamos múltiples variantes por par de ciudades
    while route_id <= NUM_ROUTES:
        for origin in CITIES:
            for destination in CITIES:
                if origin != destination and route_id <= NUM_ROUTES:
                    # Obtener distancia base de la matriz
                    base_distance = CITY_DISTANCES[origin][destination]
                    
                    # Añadir variación ±10% para simular rutas alternativas
                    distance = round(base_distance * np.random.uniform(0.9, 1.1), 2)
                    
                    # Calcular duración estimada (velocidad promedio 55-65 km/h)
                    avg_speed = np.random.uniform(55, 65)
                    estimated_duration = round(distance / avg_speed, 2)
                    
                    # Calcular costo de peajes (~$0.50-$1.50 por cada 50km)
                    toll_cost = round((distance / 50) * np.random.uniform(0.5, 1.5), 2)
                    
                    # Código de ruta único
                    route_code = f"RT-{route_id:03d}"
                    
                    routes_data.append({
                        'route_code': route_code,
                        'origin_city': origin,
                        'destination_city': destination,
                        'distance_km': distance,
                        'estimated_duration_hours': estimated_duration,
                        'toll_cost': toll_cost
                    })
                    
                    route_id += 1
    
    df = pd.DataFrame(routes_data)
    print(f"   ✓ {len(df)} rutas generadas entre 5 ciudades")
    print(f"   Rango de distancias: {df['distance_km'].min():.1f} - {df['distance_km'].max():.1f} km")
    
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# SECCIÓN 5: FUNCIÓN AUXILIAR DE DISTRIBUCIÓN HORARIA
# ═══════════════════════════════════════════════════════════════════════════════

def get_hourly_distribution():
    """
    ╔══════════════════════════════════════════════════════════════════════════╗
    ║  FUNCIÓN AUXILIAR: DISTRIBUCIÓN HORARIA DE VIAJES                        ║
    ║  Retorna array de 24 probabilidades para simular patrones operativos    ║
    ╚══════════════════════════════════════════════════════════════════════════╝
    
    EXPLICACIÓN DETALLADA:
    ──────────────────────
    Este método es FUNDAMENTAL para generar datos realistas. En lugar de
    distribuir los viajes uniformemente (4.17% por hora), simula el patrón
    real de una empresa de logística con picos y valles operativos.
    
    ╔══════════════════════════════════════════════════════════════════════════╗
    ║  DISTRIBUCIÓN POR FRANJAS HORARIAS                                       ║
    ╠══════════════════════════════════════════════════════════════════════════╣
    ║  00:00 - 05:59  MADRUGADA     │  0.5% - 2.0% por hora                   ║
    ║                                │  Actividad mínima, solo operaciones      ║
    ║                                │  nocturnas especiales o emergencias      ║
    ║────────────────────────────────┼──────────────────────────────────────────║
    ║  06:00 - 09:59  PICO MATUTINO │  7.5% - 8.5% por hora ⭐ MÁXIMO        ║
    ║                                │  Salida principal de la flota            ║
    ║                                │  Entregas programadas para la mañana     ║
    ║                                │  Aprovecha tráfico ligero                ║
    ║────────────────────────────────┼──────────────────────────────────────────║
    ║  10:00 - 11:59  MEDIA MAÑANA  │  5.5% - 6.5% por hora                   ║
    ║                                │  Continúa operación pero menor intensidad║
    ║────────────────────────────────┼──────────────────────────────────────────║
    ║  12:00 - 13:59  ALMUERZO      │  4.0% por hora                          ║
    ║                                │  Reducción por horario de comida         ║
    ║────────────────────────────────┼──────────────────────────────────────────║
    ║  14:00 - 17:59  PICO VESPERTINO│  6.0% - 7.0% por hora ⭐ SEGUNDO PICO  ║
    ║                                │  Entregas de tarde                       ║
    ║                                │  Completar rutas pendientes              ║
    ║────────────────────────────────┼──────────────────────────────────────────║
    ║  18:00 - 23:59  NOCHE         │  1.0% - 4.5% por hora                   ║
    ║                                │  Disminución progresiva                  ║
    ║                                │  Finalización de jornada                 ║
    ╚════════════════════════════════╧══════════════════════════════════════════╝
    
    VALORES EXACTOS:
    ────────────────
    Hora    Probabilidad    Justificación
    ────────────────────────────────────────────────────────────────────────
    00:00      0.5%         Mínima actividad nocturna
    01:00      0.5%         Mínima actividad nocturna
    02:00      0.5%         Mínima actividad nocturna
    03:00      0.5%         Mínima actividad nocturna
    04:00      1.0%         Inicio preparación pre-jornada
    05:00      2.0%         Preparación activa de flota
    06:00      8.0%         🔥 INICIO PICO MATUTINO
    07:00      8.5%         🔥 PICO MÁXIMO DEL DÍA
    08:00      8.5%         🔥 PICO MÁXIMO DEL DÍA
    09:00      7.5%         🔥 FIN PICO MATUTINO
    10:00      6.5%         Operación alta sostenida
    11:00      5.5%         Operación moderada-alta
    12:00      4.0%         Reducción almuerzo
    13:00      4.0%         Reducción almuerzo
    14:00      6.5%         🔥 INICIO PICO VESPERTINO
    15:00      7.0%         🔥 PICO VESPERTINO MÁXIMO
    16:00      7.0%         🔥 PICO VESPERTINO MÁXIMO
    17:00      6.0%         🔥 FIN PICO VESPERTINO
    18:00      4.5%         Inicio de disminución
    19:00      3.5%         Disminución progresiva
    20:00      2.5%         Operaciones finales
    21:00      2.0%         Finalización jornadas
    22:00      1.5%         Actividad residual
    23:00      1.0%         Mínima actividad
    ────────────────────────────────────────────────────────────────────────
    SUMA TOTAL: 100.0%
    
    USO EN generate_trips():
    ────────────────────────
    Este array se usa con np.random.choice() para seleccionar horas de salida
    de manera ponderada, creando el patrón realista de operación logística.
    
    Returns:
        numpy.array: Array de 24 valores que suman exactamente 1.0 (100%)
    """
    return np.array([
        # Madrugada (00:00 - 05:59): Actividad mínima
        0.005,  # 00:00 - 0.5%
        0.005,  # 01:00 - 0.5%
        0.005,  # 02:00 - 0.5%
        0.005,  # 03:00 - 0.5%
        0.010,  # 04:00 - 1.0%
        0.020,  # 05:00 - 2.0%
        
        # Pico Matutino (06:00 - 09:59): MÁXIMA actividad ⭐
        0.080,  # 06:00 - 8.0%
        0.085,  # 07:00 - 8.5% 🔥 PICO MÁXIMO
        0.085,  # 08:00 - 8.5% 🔥 PICO MÁXIMO
        0.075,  # 09:00 - 7.5%
        
        # Media Mañana (10:00 - 11:59): Alta actividad sostenida
        0.065,  # 10:00 - 6.5%
        0.055,  # 11:00 - 5.5%
        
        # Almuerzo (12:00 - 13:59): Reducción
        0.040,  # 12:00 - 4.0%
        0.040,  # 13:00 - 4.0%
        
        # Pico Vespertino (14:00 - 17:59): SEGUNDO pico ⭐
        0.065,  # 14:00 - 6.5%
        0.070,  # 15:00 - 7.0% 🔥
        0.070,  # 16:00 - 7.0% 🔥
        0.060,  # 17:00 - 6.0%
        
        # Noche (18:00 - 23:59): Disminución progresiva
        0.045,  # 18:00 - 4.5%
        0.035,  # 19:00 - 3.5%
        0.025,  # 20:00 - 2.5%
        0.020,  # 21:00 - 2.0%
        0.015,  # 22:00 - 1.5%
        0.020   # 23:00 - 2.0% (ajustado para sumar 100%)
    ])


# ═══════════════════════════════════════════════════════════════════════════════
# SECCIÓN 6: GENERACIÓN DE TABLA TRANSACCIONAL TRIPS
# ═══════════════════════════════════════════════════════════════════════════════

def generate_trips(vehicles_df, drivers_df, routes_df):
    """
    ╔══════════════════════════════════════════════════════════════════════════╗
    ║  FUNCIÓN PRINCIPAL: GENERACIÓN DE VIAJES (TRIPS)                         ║
    ║  Genera 100,000 viajes que representan 2 años de operación (2024-2025)  ║
    ╚══════════════════════════════════════════════════════════════════════════╝
    
    EXPLICACIÓN DETALLADA DEL ALGORITMO:
    ────────────────────────────────────
    Este es el método MÁS COMPLEJO del sistema. Genera 100,000 viajes que
    representan la operación completa de FleetLogix durante 2 años, manteniendo
    coherencia total con las reglas de negocio y física del mundo real.
    
    ╔══════════════════════════════════════════════════════════════════════════╗
    ║  PROCESO EN 7 PASOS DETALLADOS                                           ║
    ╚══════════════════════════════════════════════════════════════════════════╝
    
    ┌────────────────────────────────────────────────────────────────────────┐
    │ PASO 1: SELECCIÓN DE FECHA Y HORA                                       │
    └────────────────────────────────────────────────────────────────────────┘
    
    • FECHA: Distribución uniforme entre 2024-01-01 y 2025-12-31
      - Representa 2 años completos de operación histórica
      - Cada día tiene igual probabilidad
      - Total: 731 días (2024 es bisiesto)
    
    • HORA: Distribución NO uniforme usando get_hourly_distribution()
      - NO se usa distribución uniforme (sería poco realista)
      - Se usa np.random.choice() con probabilidades ponderadas
      - Picos matutinos (7-8am) representan 8.5% de viajes cada uno
      - Madrugadas (0-5am) representan solo 0.5-2% cada una
      - ESTO CREA EL PATRÓN REALISTA DE OPERACIÓN
    
    ┌────────────────────────────────────────────────────────────────────────┐
    │ PASO 2: ASIGNACIÓN DE FOREIGN KEYS                                      │
    └────────────────────────────────────────────────────────────────────────┘
    
    • vehicle_id: Entero aleatorio entre 1 y 200
      - Referencia a tabla vehicles (vehicle_id como PK)
      - Mantiene integridad referencial
      - Algunos vehículos tendrán más viajes que otros (realista)
    
    • driver_id: Entero aleatorio entre 1 y 400
      - Referencia a tabla drivers (driver_id como PK)
      - 400 conductores para 200 vehículos = turnos compartidos
      - Realista: múltiples conductores por vehículo
    
    • route_id: Entero aleatorio entre 1 y 50
      - Referencia a tabla routes (route_id como PK)
      - Algunas rutas más populares que otras (distribución natural)
    
    ┌────────────────────────────────────────────────────────────────────────┐
    │ PASO 3: RECUPERACIÓN DE DATOS DE RUTA                                   │
    └────────────────────────────────────────────────────────────────────────┘
    
    Usando el route_id seleccionado, se recupera:
    • distance_km: Distancia real de la ruta
    • estimated_duration_hours: Duración base estimada
    
    Estos valores se usan para calcular arrival_datetime y fuel_consumed
    
    ┌────────────────────────────────────────────────────────────────────────┐
    │ PASO 4: CÁLCULO DE HORA DE LLEGADA (arrival_datetime)                   │
    └────────────────────────────────────────────────────────────────────────┘
    
    FÓRMULA CLAVE PARA CONSISTENCIA TEMPORAL:
    
        actual_duration = estimated_duration × factor_variación
        factor_variación = random(0.8, 1.2)
        
        arrival_datetime = departure_datetime + actual_duration
    
    JUSTIFICACIÓN:
    • estimated_duration es la duración base de la ruta (de tabla routes)
    • factor_variación (0.8 a 1.2) simula:
      - Tráfico ligero/pesado
      - Condiciones climáticas
      - Habilidad del conductor
      - Retrasos en entregas
    
    • factor_variación es SIEMPRE POSITIVO (0.8 a 1.2)
    • Por lo tanto: arrival_datetime > departure_datetime SIEMPRE
    
    ⚠️ ESTO GARANTIZA CONSISTENCIA TEMPORAL: arrival > departure
    
    ┌────────────────────────────────────────────────────────────────────────┐
    │ PASO 5: CÁLCULO DE COMBUSTIBLE CONSUMIDO                                │
    └────────────────────────────────────────────────────────────────────────┘
    
    FÓRMULA:
    
        fuel_consumed = (distance_km / km_per_liter) × random(0.9, 1.1)
    
    RENDIMIENTO POR TIPO DE VEHÍCULO:
    • Camión Grande:   3.5 km/L  (mayor consumo)
    • Camión Mediano:  5.0 km/L
    • Van:             8.0 km/L
    • Motocicleta:    25.0 km/L  (menor consumo)
    
    JUSTIFICACIÓN DEL FACTOR (0.9, 1.1):
    • Variación ±10% simula:
      - Estilo de conducción
      - Peso de la carga
      - Condiciones del terreno
      - Mantenimiento del vehículo
    
    EJEMPLO:
    Camión Grande recorre 150 km:
    - Consumo base: 150 / 3.5 = 42.86 litros
    - Con variación: 38.57 a 47.14 litros (±10%)
    
    ┌────────────────────────────────────────────────────────────────────────┐
    │ PASO 6: ASIGNACIÓN DE PESO DE CARGA                                     │
    └────────────────────────────────────────────────────────────────────────┘
    
    FÓRMULA:
    
        total_weight_kg = capacity_kg × load_factor
        load_factor = random(0.5, 0.95)
    
    JUSTIFICACIÓN:
    • Los vehículos NUNCA van vacíos (mínimo 50% cargados)
    • Los vehículos RARA VEZ van al 100% (máximo 95%)
    • Esto es REALISTA para operaciones logísticas:
      - Espacio desaprovechado
      - Optimización de rutas
      - Múltiples entregas con pesos variables
    
    EJEMPLO:
    Van con capacidad 1500 kg:
    - Peso cargado: 750 kg a 1425 kg
    - Promedio: ~1087 kg (72.5% de capacidad)
    
    ┌────────────────────────────────────────────────────────────────────────┐
    │ PASO 7: ASIGNACIÓN DE ESTADO DEL VIAJE                                  │
    └────────────────────────────────────────────────────────────────────────┘
    
    DISTRIBUCIÓN DE ESTADOS:
    • completed:    95% - Viaje completado exitosamente
    • in_progress:   3% - Viaje en curso (arrival = NULL)
    • cancelled:     2% - Viaje cancelado
    
    REGLAS DE NEGOCIO:
    • Si status = 'in_progress': arrival_datetime = NULL
    • Si status = 'cancelled': 50% probabilidad de arrival = NULL
      (algunos fueron cancelados después de iniciar)
    • Si status = 'completed': arrival_datetime siempre tiene valor
    
    ╔══════════════════════════════════════════════════════════════════════════╗
    ║  GARANTÍAS DE CONSISTENCIA                                               ║
    ╚══════════════════════════════════════════════════════════════════════════╝
    
    ✓ Integridad Referencial:
      - Todos los vehicle_id existen en tabla vehicles (1-200)
      - Todos los driver_id existen en tabla drivers (1-400)
      - Todos los route_id existen en tabla routes (1-50)
    
    ✓ Consistencia Temporal:
      - arrival_datetime > departure_datetime SIEMPRE (cuando no NULL)
      - Fechas dentro de rango operativo (2024-2025)
      - Horas siguen distribución realista
    
    ✓ Coherencia Física:
      - Consumo combustible proporcional a distancia y tipo vehículo
      - Peso cargado nunca excede capacidad
      - Duración del viaje proporcional a distancia
    
    ✓ Reglas de Negocio:
      - Viajes "in_progress" no tienen hora de llegada
      - Viajes "completed" siempre tienen hora de llegada
      - 95% de viajes se completan exitosamente (realista)
    
    Args:
        vehicles_df (pd.DataFrame): DataFrame con vehículos generados
        drivers_df (pd.DataFrame): DataFrame con conductores generados
        routes_df (pd.DataFrame): DataFrame con rutas generadas
    
    Returns:
        pd.DataFrame: DataFrame con 100,000 viajes coherentes y realistas
    """
    
    print("\n🚛 Generando viajes (esto puede tomar 1-2 minutos)...")
    print("   Creando 100,000 viajes con:")
    print("   • Distribución horaria realista (picos matutinos y vespertinos)")
    print("   • Consistencia temporal (arrival > departure)")
    print("   • Consumo de combustible por tipo de vehículo")
    print("   • Factor de carga 50-95%")
    
    trips_data = []
    
    # Obtener distribución horaria realista
    hourly_probs = get_hourly_distribution()
    
    # Crear diccionario de vehículos para lookup rápido
    vehicle_lookup = vehicles_df.set_index(vehicles_df.index + 1)[['vehicle_type', 'capacity_kg']].to_dict('index')
    
    # Fechas de inicio y fin del período operativo
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2025, 12, 31, 23, 59, 59)
    total_seconds = int((end_date - start_date).total_seconds())
    
    # Generar NUM_TRIPS viajes
    for i in range(NUM_TRIPS):
        # Mostrar progreso cada 10,000 registros
        if (i + 1) % 10000 == 0:
            print(f"   Progreso: {i + 1:,}/{NUM_TRIPS:,} viajes generados...")
        
        # ═══════════════════════════════════════════════════════════════════
        # PASO 1: Seleccionar fecha y hora de salida
        # ═══════════════════════════════════════════════════════════════════
        
        # Fecha aleatoria entre 2024-01-01 y 2025-12-31
        random_seconds = np.random.randint(0, total_seconds)
        departure_datetime = start_date + timedelta(seconds=int(random_seconds))
        
        # Ajustar hora usando distribución horaria ponderada
        selected_hour = np.random.choice(24, p=hourly_probs)
        departure_datetime = departure_datetime.replace(
            hour=selected_hour,
            minute=np.random.randint(0, 60),
            second=np.random.randint(0, 60)
        )
        
        # ═══════════════════════════════════════════════════════════════════
        # PASO 2: Asignar Foreign Keys
        # ═══════════════════════════════════════════════════════════════════
        
        vehicle_id = np.random.randint(1, NUM_VEHICLES + 1)
        driver_id = np.random.randint(1, NUM_DRIVERS + 1)
        route_id = np.random.randint(1, len(routes_df) + 1)
        
        # ═══════════════════════════════════════════════════════════════════
        # PASO 3: Recuperar datos de la ruta seleccionada
        # ═══════════════════════════════════════════════════════════════════
        
        route = routes_df.iloc[route_id - 1]
        distance_km = route['distance_km']
        estimated_duration_hours = route['estimated_duration_hours']
        
        # ═══════════════════════════════════════════════════════════════════
        # PASO 4: Calcular hora de llegada con consistencia temporal
        # ═══════════════════════════════════════════════════════════════════
        
        # Duración real varía ±20% de la estimada (tráfico, clima, etc.)
        actual_duration_hours = estimated_duration_hours * np.random.uniform(0.8, 1.2)
        duration_timedelta = timedelta(hours=actual_duration_hours)
        
        # arrival = departure + duración (SIEMPRE mayor que departure)
        arrival_datetime = departure_datetime + duration_timedelta
        
        # ═══════════════════════════════════════════════════════════════════
        # PASO 5: Calcular consumo de combustible según tipo de vehículo
        # ═══════════════════════════════════════════════════════════════════
        
        vehicle_info = vehicle_lookup[vehicle_id]
        vehicle_type = vehicle_info['vehicle_type']
        km_per_liter = VEHICLE_TYPES[vehicle_type]['km_per_liter']
        
        # Consumo base con variación ±10%
        fuel_consumed = (distance_km / km_per_liter) * np.random.uniform(0.9, 1.1)
        fuel_consumed = round(fuel_consumed, 2)
        
        # ═══════════════════════════════════════════════════════════════════
        # PASO 6: Calcular peso de carga (50% a 95% de capacidad)
        # ═══════════════════════════════════════════════════════════════════
        
        capacity_kg = vehicle_info['capacity_kg']
        load_factor = np.random.uniform(0.5, 0.95)
        total_weight_kg = round(capacity_kg * load_factor, 2)
        
        # ═══════════════════════════════════════════════════════════════════
        # PASO 7: Asignar estado del viaje
        # ═══════════════════════════════════════════════════════════════════
        
        # 95% completed, 3% in_progress, 2% cancelled
        status = np.random.choice(
            ['completed', 'in_progress', 'cancelled'],
            p=[0.95, 0.03, 0.02]
        )
        
        # Aplicar reglas de negocio para arrival_datetime según status
        if status == 'in_progress':
            # Viajes en progreso no tienen hora de llegada
            arrival_datetime = None
        elif status == 'cancelled':
            # 50% de cancelados no llegaron, 50% sí (cancelados después)
            if np.random.random() < 0.5:
                arrival_datetime = None
        
        # Agregar viaje a la lista
        trips_data.append({
            'vehicle_id': vehicle_id,
            'driver_id': driver_id,
            'route_id': route_id,
            'departure_datetime': departure_datetime,
            'arrival_datetime': arrival_datetime,
            'fuel_consumed_liters': fuel_consumed,
            'total_weight_kg': total_weight_kg,
            'status': status
        })
    
    df = pd.DataFrame(trips_data)
    
    # Estadísticas finales
    print(f"\n   ✓ {len(df):,} viajes generados exitosamente")
    print(f"   Distribución de estados:")
    print(f"   • Completados: {len(df[df['status']=='completed']):,} ({len(df[df['status']=='completed'])/len(df)*100:.1f}%)")
    print(f"   • En progreso: {len(df[df['status']=='in_progress']):,} ({len(df[df['status']=='in_progress'])/len(df)*100:.1f}%)")
    print(f"   • Cancelados: {len(df[df['status']=='cancelled']):,} ({len(df[df['status']=='cancelled'])/len(df)*100:.1f}%)")
    
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# SECCIÓN 7: GENERACIÓN DE TABLA TRANSACCIONAL DELIVERIES
# ═══════════════════════════════════════════════════════════════════════════════

def generate_deliveries(trips_df):
    """
    Genera exactamente 400,000 entregas (2-6 entregas por viaje, 4 más probable)
    
    Distribución de entregas por viaje:
    - 2 entregas: 10%
    - 3 entregas: 20%
    - 4 entregas: 40% ⭐ MÁS COMÚN
    - 5 entregas: 20%
    - 6 entregas: 10%
    
    Características:
    - Tracking number único (formato: DOM{trip_id:06d}{seq:02d}{random:04d})
    - Pesos distribuidos proporcionalmente del peso total del viaje
    - Horarios escalonados durante la duración del viaje
    - Estados: delivered (85%), pending (10%), failed (5%)
    - 85% de entregados tienen firma del receptor
    
    Args:
        trips_df (pd.DataFrame): DataFrame con viajes generados
    
    Returns:
        pd.DataFrame: DataFrame con exactamente 400,000 entregas
    """
    print("\n📦 Generando entregas individuales...")
    print(f"   Objetivo: {NUM_DELIVERIES:,} entregas exactas")
    print("   Distribución: 2-6 entregas por viaje (4 más probable)")
    
    deliveries_data = []
    
    # Distribución de número de entregas por viaje
    # Promedio esperado: 2*0.10 + 3*0.20 + 4*0.40 + 5*0.20 + 6*0.10 = 4.0
    deliveries_per_trip_options = [2, 3, 4, 5, 6]
    deliveries_per_trip_probs = [0.10, 0.20, 0.40, 0.20, 0.10]
    
    # Pre-calcular cuántas entregas tendrá cada viaje para llegar a exactamente 400,000
    num_trips = len(trips_df)
    target_deliveries = NUM_DELIVERIES
    
    # Generar distribución inicial
    deliveries_per_trip = np.random.choice(
        deliveries_per_trip_options, 
        size=num_trips, 
        p=deliveries_per_trip_probs
    )
    
    # Ajustar para llegar exactamente a 400,000
    current_total = deliveries_per_trip.sum()
    diff = target_deliveries - current_total
    
    if diff > 0:
        # Necesitamos agregar entregas - incrementar algunos viajes que tienen < 6
        indices = np.where(deliveries_per_trip < 6)[0]
        np.random.shuffle(indices)
        for i in indices[:abs(diff)]:
            deliveries_per_trip[i] += 1
    elif diff < 0:
        # Necesitamos quitar entregas - decrementar algunos viajes que tienen > 2
        indices = np.where(deliveries_per_trip > 2)[0]
        np.random.shuffle(indices)
        for i in indices[:abs(diff)]:
            deliveries_per_trip[i] -= 1
    
    for idx, trip in trips_df.iterrows():
        trip_id = idx + 1
        
        # Número de entregas pre-calculado para este viaje
        num_deliveries = deliveries_per_trip[idx]
        
        # Dividir peso total entre entregas (con variación)
        weights = np.random.dirichlet(np.ones(num_deliveries)) * trip['total_weight_kg']
        
        # Calcular intervalos de tiempo si el viaje tiene arrival
        if pd.notna(trip['arrival_datetime']):
            total_duration = (trip['arrival_datetime'] - trip['departure_datetime']).total_seconds()
            time_intervals = np.sort(np.random.uniform(0, total_duration, num_deliveries))
        else:
            time_intervals = None
        
        # Generar cada entrega
        for seq in range(num_deliveries):
            # Tracking number único: DOM + trip_id (6 dígitos) + secuencia (2 dígitos) + random (4 dígitos)
            tracking_number = f"DOM{trip_id:06d}{seq+1:02d}{np.random.randint(1000, 9999)}"
            
            # Nombre de cliente
            customer_name = fake.name()
            
            # Dirección de entrega
            delivery_address = fake.address().replace('\n', ', ')
            
            # Peso del paquete (mínimo 0.1 kg para evitar pesos <= 0)
            package_weight = max(0.1, round(weights[seq], 2))
            
            # Horario programado de entrega
            if time_intervals is not None:
                scheduled_datetime = trip['departure_datetime'] + timedelta(seconds=time_intervals[seq])
            else:
                # Para viajes sin arrival, programar en futuro cercano
                scheduled_datetime = trip['departure_datetime'] + timedelta(hours=np.random.uniform(1, 8))
            
            # Estado de la entrega: delivered (85%), pending (10%), failed (5%)
            # Solo viajes completados pueden tener entregas delivered
            if trip['status'] == 'completed':
                delivery_status = np.random.choice(
                    ['delivered', 'pending', 'failed'],
                    p=[0.85, 0.10, 0.05]
                )
            elif trip['status'] == 'in_progress':
                # Viajes en progreso: entregas pendientes o algunas entregadas
                delivery_status = np.random.choice(
                    ['delivered', 'pending'],
                    p=[0.30, 0.70]
                )
            else:  # cancelled
                # Viajes cancelados: pendientes o fallidas
                delivery_status = np.random.choice(
                    ['pending', 'failed'],
                    p=[0.60, 0.40]
                )
            
            # Fecha y hora de entrega real (solo si delivered)
            if delivery_status == 'delivered':
                # Entrega entre 0 y +60 minutos del horario programado (nunca antes)
                delivered_datetime = scheduled_datetime + timedelta(minutes=np.random.randint(0, 61))
            else:
                delivered_datetime = None
            
            # Firma del receptor (85% de los entregados tienen firma)
            if delivery_status == 'delivered':
                recipient_signature = np.random.choice([True, False], p=[0.85, 0.15])
            else:
                recipient_signature = False
            
            deliveries_data.append({
                'trip_id': trip_id,
                'tracking_number': tracking_number,
                'customer_name': customer_name,
                'delivery_address': delivery_address,
                'package_weight_kg': package_weight,
                'scheduled_datetime': scheduled_datetime,
                'delivered_datetime': delivered_datetime,
                'delivery_status': delivery_status,
                'recipient_signature': recipient_signature
            })
        
        # Mostrar progreso cada 10,000 viajes procesados
        if (trip_id) % 10000 == 0:
            print(f"   Procesados {trip_id:,}/{len(trips_df):,} viajes...")
    
    df = pd.DataFrame(deliveries_data)
    
    print(f"\n   ✓ {len(df):,} entregas generadas")
    print(f"   Promedio: {len(df)/len(trips_df):.2f} entregas por viaje")
    print(f"   Distribución de estados:")
    print(f"   • Entregadas: {len(df[df['delivery_status']=='delivered']):,} ({len(df[df['delivery_status']=='delivered'])/len(df)*100:.1f}%)")
    print(f"   • Pendientes: {len(df[df['delivery_status']=='pending']):,} ({len(df[df['delivery_status']=='pending'])/len(df)*100:.1f}%)")
    print(f"   • Fallidas: {len(df[df['delivery_status']=='failed']):,} ({len(df[df['delivery_status']=='failed'])/len(df)*100:.1f}%)")
    
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# SECCIÓN 8: GENERACIÓN DE TABLA TRANSACCIONAL MAINTENANCE
# ═══════════════════════════════════════════════════════════════════════════════

def generate_maintenance(trips_df, vehicles_df):
    """
    Genera exactamente 5,000 registros de mantenimiento (~1 por cada 20 viajes por vehículo)
    
    Tipos de mantenimiento con probabilidades:
    - Cambio de aceite: 30% ($50-$150)
    - Revisión de frenos: 15% ($100-$300)
    - Cambio de llantas: 10% ($200-$800)
    - Mantenimiento general: 20% ($150-$500)
    - Revisión de motor: 10% ($200-$600)
    - Alineación y balanceo: 15% ($50-$120)
    
    Lógica:
    - Se calcula cuántos viajes ha hecho cada vehículo
    - Cada ~20 viajes se programa un mantenimiento
    - Fechas coherentes con el historial de viajes del vehículo
    - Próximo mantenimiento programado entre 75-105 días después
    - Se ajusta la distribución para alcanzar exactamente 5,000 registros
    
    Args:
        trips_df (pd.DataFrame): DataFrame con viajes generados
        vehicles_df (pd.DataFrame): DataFrame con vehículos generados
    
    Returns:
        pd.DataFrame: DataFrame con exactamente 5,000 mantenimientos
    """
    print("\n🔧 Generando registros de mantenimiento...")
    print(f"   Objetivo: {NUM_MAINTENANCE:,} registros exactos")
    print("   Frecuencia: ~1 mantenimiento por cada 20 viajes por vehículo")
    
    maintenance_data = []
    
    # Contar viajes por vehículo
    trips_per_vehicle = trips_df.groupby('vehicle_id').size().to_dict()
    
    # Obtener fechas de viajes por vehículo
    vehicle_trip_dates = trips_df.groupby('vehicle_id')['departure_datetime'].apply(list).to_dict()
    
    # Preparar tipos y probabilidades de mantenimiento
    maintenance_types = list(MAINTENANCE_TYPES.keys())
    maintenance_probs = [MAINTENANCE_TYPES[mt]['probability'] for mt in maintenance_types]
    
    # Calcular distribución de mantenimientos para cada vehículo para llegar a exactamente 5,000
    # Base: NUM_MAINTENANCE / NUM_VEHICLES = 5000 / 200 = 25 por vehículo en promedio
    target_total = NUM_MAINTENANCE
    base_per_vehicle = target_total // NUM_VEHICLES  # 25
    extra_count = target_total % NUM_VEHICLES  # 0 en este caso
    
    # Asignar mantenimientos por vehículo (proporcional a sus viajes)
    total_trips = sum(trips_per_vehicle.values())
    maintenance_counts = {}
    
    for vehicle_id in range(1, NUM_VEHICLES + 1):
        num_trips = trips_per_vehicle.get(vehicle_id, 0)
        # Proporción de viajes de este vehículo
        proportion = num_trips / total_trips if total_trips > 0 else 1/NUM_VEHICLES
        maintenance_counts[vehicle_id] = max(1, int(target_total * proportion))
    
    # Ajustar para llegar exactamente a 5,000
    current_total = sum(maintenance_counts.values())
    diff = target_total - current_total
    
    vehicle_ids = list(range(1, NUM_VEHICLES + 1))
    if diff > 0:
        # Agregar mantenimientos a vehículos aleatorios
        selected = np.random.choice(vehicle_ids, size=abs(diff), replace=True)
        for vid in selected:
            maintenance_counts[vid] += 1
    elif diff < 0:
        # Quitar mantenimientos de vehículos con más de 1
        candidates = [v for v in vehicle_ids if maintenance_counts[v] > 1]
        selected = np.random.choice(candidates, size=min(abs(diff), len(candidates)), replace=False)
        for vid in selected:
            maintenance_counts[vid] -= 1
    
    for vehicle_id in range(1, NUM_VEHICLES + 1):
        # Número de mantenimientos asignados a este vehículo
        num_maintenances = maintenance_counts.get(vehicle_id, 1)
        
        # Obtener fechas de viajes de este vehículo
        trip_dates = vehicle_trip_dates.get(vehicle_id, [])
        if not trip_dates:
            # Si no hay viajes, usar fechas del período operativo
            min_date = START_DATE.date()
            max_date = END_DATE.date()
        else:
            min_date = min(trip_dates).date()
            max_date = max(trip_dates).date()
        
        # Generar mantenimientos
        for _ in range(num_maintenances):
            # Fecha de mantenimiento entre los viajes del vehículo
            days_range = (max_date - min_date).days
            if days_range > 0:
                maintenance_date = min_date + timedelta(days=np.random.randint(0, days_range))
            else:
                maintenance_date = min_date
            
            # Seleccionar tipo de mantenimiento según probabilidades
            maintenance_type = np.random.choice(maintenance_types, p=maintenance_probs)
            
            # Costo según el rango del tipo de mantenimiento
            cost_min, cost_max = MAINTENANCE_TYPES[maintenance_type]['cost_range']
            cost = round(np.random.uniform(cost_min, cost_max), 2)
            
            # Descripción genérica
            description = f"{maintenance_type} programado para vehículo #{vehicle_id}"
            
            # Próximo mantenimiento: 75-105 días después
            next_maintenance_date = maintenance_date + timedelta(days=np.random.randint(75, 105))
            
            # Proveedor de mantenimiento
            performed_by = np.random.choice(MAINTENANCE_PROVIDERS)
            
            maintenance_data.append({
                'vehicle_id': vehicle_id,
                'maintenance_date': maintenance_date,
                'maintenance_type': maintenance_type,
                'description': description,
                'cost': cost,
                'next_maintenance_date': next_maintenance_date,
                'performed_by': performed_by
            })
    
    df = pd.DataFrame(maintenance_data)
    
    print(f"   ✓ {len(df):,} registros de mantenimiento generados")
    print(f"   Promedio: {len(df)/NUM_VEHICLES:.1f} mantenimientos por vehículo")
    print(f"   Tipos más frecuentes:")
    top_types = df['maintenance_type'].value_counts().head(3)
    for mtype, count in top_types.items():
        print(f"   • {mtype}: {count:,} ({count/len(df)*100:.1f}%)")
    
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# SECCIÓN 9: VALIDACIÓN EXHAUSTIVA DE DATOS
# ═══════════════════════════════════════════════════════════════════════════════

def validate_data():
    """
    Realiza validación exhaustiva de consistencia y lógica de los datos cargados
    
    Validaciones realizadas:
    1. Conteos de registros por tabla
    2. Integridad referencial (todos los FKs válidos)
    3. Consistencia temporal (arrival > departure)
    4. Constraints únicos (license_plate, employee_code, etc.)
    5. Coherencia de pesos (suma deliveries ≤ trip weight)
    6. Rangos lógicos (capacidades, combustible, distancias)
    7. Fechas válidas (no futuras, licencias no expiradas)
    """
    print("\n" + "═" * 80)
    print("VALIDACIÓN EXHAUSTIVA DE DATOS")
    print("═" * 80)
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        validation_passed = True
        
        # ─────────────────────────────────────────────────────────────────────
        # VALIDACIÓN 1: Conteos de Registros
        # ─────────────────────────────────────────────────────────────────────
        print("\n[1] Validando conteos de registros...")
        
        tables = ['vehicles', 'drivers', 'routes', 'trips', 'deliveries', 'maintenance']
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"    ✓ {table}: {count:,} registros")
        
        # ─────────────────────────────────────────────────────────────────────
        # VALIDACIÓN 2: Integridad Referencial - trips → vehicles
        # ─────────────────────────────────────────────────────────────────────
        print("\n[2] Validando integridad referencial...")
        
        cursor.execute("""
            SELECT COUNT(*) FROM trips t
            WHERE NOT EXISTS (SELECT 1 FROM vehicles v WHERE v.vehicle_id = t.vehicle_id)
        """)
        invalid_vehicles = cursor.fetchone()[0]
        if invalid_vehicles > 0:
            print(f"    ❌ FALLO: {invalid_vehicles} viajes con vehicle_id inválido")
            validation_passed = False
        else:
            print(f"    ✓ trips → vehicles: Todos los vehicle_id son válidos")
        
        # trips → drivers
        cursor.execute("""
            SELECT COUNT(*) FROM trips t
            WHERE NOT EXISTS (SELECT 1 FROM drivers d WHERE d.driver_id = t.driver_id)
        """)
        invalid_drivers = cursor.fetchone()[0]
        if invalid_drivers > 0:
            print(f"    ❌ FALLO: {invalid_drivers} viajes con driver_id inválido")
            validation_passed = False
        else:
            print(f"    ✓ trips → drivers: Todos los driver_id son válidos")
        
        # trips → routes
        cursor.execute("""
            SELECT COUNT(*) FROM trips t
            WHERE NOT EXISTS (SELECT 1 FROM routes r WHERE r.route_id = t.route_id)
        """)
        invalid_routes = cursor.fetchone()[0]
        if invalid_routes > 0:
            print(f"    ❌ FALLO: {invalid_routes} viajes con route_id inválido")
            validation_passed = False
        else:
            print(f"    ✓ trips → routes: Todos los route_id son válidos")
        
        # deliveries → trips
        cursor.execute("""
            SELECT COUNT(*) FROM deliveries d
            WHERE NOT EXISTS (SELECT 1 FROM trips t WHERE t.trip_id = d.trip_id)
        """)
        invalid_trip_refs = cursor.fetchone()[0]
        if invalid_trip_refs > 0:
            print(f"    ❌ FALLO: {invalid_trip_refs} entregas con trip_id inválido")
            validation_passed = False
        else:
            print(f"    ✓ deliveries → trips: Todos los trip_id son válidos")
        
        # maintenance → vehicles
        cursor.execute("""
            SELECT COUNT(*) FROM maintenance m
            WHERE NOT EXISTS (SELECT 1 FROM vehicles v WHERE v.vehicle_id = m.vehicle_id)
        """)
        invalid_maint_vehicles = cursor.fetchone()[0]
        if invalid_maint_vehicles > 0:
            print(f"    ❌ FALLO: {invalid_maint_vehicles} mantenimientos con vehicle_id inválido")
            validation_passed = False
        else:
            print(f"    ✓ maintenance → vehicles: Todos los vehicle_id son válidos")
        
        # ─────────────────────────────────────────────────────────────────────
        # VALIDACIÓN 3: Consistencia Temporal
        # ─────────────────────────────────────────────────────────────────────
        print("\n[3] Validando consistencia temporal...")
        
        cursor.execute("""
            SELECT COUNT(*) FROM trips 
            WHERE arrival_datetime IS NOT NULL 
            AND arrival_datetime <= departure_datetime
        """)
        temporal_errors = cursor.fetchone()[0]
        if temporal_errors > 0:
            print(f"    ❌ FALLO: {temporal_errors} viajes con arrival <= departure")
            validation_passed = False
        else:
            print(f"    ✓ Todos los viajes tienen arrival_datetime > departure_datetime")
        
        # Verificar que no hay fechas futuras
        cursor.execute("""
            SELECT COUNT(*) FROM trips 
            WHERE departure_datetime > CURRENT_TIMESTAMP
        """)
        future_trips = cursor.fetchone()[0]
        if future_trips > 0:
            print(f"    ❌ FALLO: {future_trips} viajes con fechas futuras")
            validation_passed = False
        else:
            print(f"    ✓ No hay viajes con fechas futuras")
        
        # ─────────────────────────────────────────────────────────────────────
        # VALIDACIÓN 4: Constraints Únicos
        # ─────────────────────────────────────────────────────────────────────
        print("\n[4] Validando constraints únicos...")
        
        # license_plate único
        cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT license_plate FROM vehicles 
                GROUP BY license_plate 
                HAVING COUNT(*) > 1
            ) duplicates
        """)
        dup_plates = cursor.fetchone()[0]
        if dup_plates > 0:
            print(f"    ❌ FALLO: {dup_plates} placas duplicadas")
            validation_passed = False
        else:
            print(f"    ✓ Todas las placas de vehículos son únicas")
        
        # employee_code único
        cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT employee_code FROM drivers 
                GROUP BY employee_code 
                HAVING COUNT(*) > 1
            ) duplicates
        """)
        dup_emp_codes = cursor.fetchone()[0]
        if dup_emp_codes > 0:
            print(f"    ❌ FALLO: {dup_emp_codes} códigos de empleado duplicados")
            validation_passed = False
        else:
            print(f"    ✓ Todos los códigos de empleado son únicos")
        
        # tracking_number único
        cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT tracking_number FROM deliveries 
                GROUP BY tracking_number 
                HAVING COUNT(*) > 1
            ) duplicates
        """)
        dup_tracking = cursor.fetchone()[0]
        if dup_tracking > 0:
            print(f"    ❌ FALLO: {dup_tracking} números de tracking duplicados")
            validation_passed = False
        else:
            print(f"    ✓ Todos los números de tracking son únicos")
        
        # ─────────────────────────────────────────────────────────────────────
        # VALIDACIÓN 5: Coherencia de Pesos
        # ─────────────────────────────────────────────────────────────────────
        print("\n[5] Validando coherencia de pesos...")
        
        cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT 
                    t.trip_id,
                    t.total_weight_kg as trip_weight,
                    COALESCE(SUM(d.package_weight_kg), 0) as deliveries_weight
                FROM trips t
                LEFT JOIN deliveries d ON t.trip_id = d.trip_id
                GROUP BY t.trip_id, t.total_weight_kg
                HAVING COALESCE(SUM(d.package_weight_kg), 0) > t.total_weight_kg * 1.01
            ) weight_errors
        """)
        weight_errors = cursor.fetchone()[0]
        if weight_errors > 0:
            print(f"    ❌ FALLO: {weight_errors} viajes donde suma de entregas > peso del viaje")
            validation_passed = False
        else:
            print(f"    ✓ En todos los viajes: suma(pesos entregas) ≤ peso total del viaje")
        
        # ─────────────────────────────────────────────────────────────────────
        # VALIDACIÓN 6: Rangos Lógicos
        # ─────────────────────────────────────────────────────────────────────
        print("\n[6] Validando rangos lógicos...")
        
        # Capacidades de vehículos
        cursor.execute("""
            SELECT COUNT(*) FROM vehicles 
            WHERE capacity_kg <= 0 OR capacity_kg > 15000
        """)
        invalid_capacities = cursor.fetchone()[0]
        if invalid_capacities > 0:
            print(f"    ❌ FALLO: {invalid_capacities} vehículos con capacidad fuera de rango")
            validation_passed = False
        else:
            print(f"    ✓ Todas las capacidades de vehículos están en rango válido")
        
        # Consumo de combustible
        cursor.execute("""
            SELECT COUNT(*) FROM trips 
            WHERE fuel_consumed_liters <= 0 OR fuel_consumed_liters > 1000
        """)
        invalid_fuel = cursor.fetchone()[0]
        if invalid_fuel > 0:
            print(f"    ❌ FALLO: {invalid_fuel} viajes con consumo de combustible fuera de rango")
            validation_passed = False
        else:
            print(f"    ✓ Todos los consumos de combustible están en rango válido")
        
        # Distancias de rutas
        cursor.execute("""
            SELECT COUNT(*) FROM routes 
            WHERE distance_km <= 0 OR distance_km > 500
        """)
        invalid_distances = cursor.fetchone()[0]
        if invalid_distances > 0:
            print(f"    ❌ FALLO: {invalid_distances} rutas con distancia fuera de rango")
            validation_passed = False
        else:
            print(f"    ✓ Todas las distancias de rutas están en rango válido")
        
        # Pesos de viajes
        cursor.execute("""
            SELECT COUNT(*) FROM trips 
            WHERE total_weight_kg <= 0 OR total_weight_kg > 15000
        """)
        invalid_trip_weights = cursor.fetchone()[0]
        if invalid_trip_weights > 0:
            print(f"    ❌ FALLO: {invalid_trip_weights} viajes con peso fuera de rango")
            validation_passed = False
        else:
            print(f"    ✓ Todos los pesos de viajes están en rango válido")
        
        # ─────────────────────────────────────────────────────────────────────
        # VALIDACIÓN 7: Fechas Válidas
        # ─────────────────────────────────────────────────────────────────────
        print("\n[7] Validando fechas...")
        
        # Licencias no expiradas en el período operativo
        cursor.execute("""
            SELECT COUNT(*) FROM drivers 
            WHERE license_expiry < '2024-01-01'
        """)
        expired_licenses = cursor.fetchone()[0]
        if expired_licenses > 0:
            print(f"    ❌ FALLO: {expired_licenses} conductores con licencia expirada antes del período operativo")
            validation_passed = False
        else:
            print(f"    ✓ Todas las licencias son válidas durante el período operativo")
        
        # Fechas de mantenimiento coherentes
        cursor.execute("""
            SELECT COUNT(*) FROM maintenance 
            WHERE maintenance_date > next_maintenance_date
        """)
        invalid_maint_dates = cursor.fetchone()[0]
        if invalid_maint_dates > 0:
            print(f"    ❌ FALLO: {invalid_maint_dates} mantenimientos con fecha > próximo mantenimiento")
            validation_passed = False
        else:
            print(f"    ✓ Todas las fechas de mantenimiento son coherentes")
        
        cursor.close()
        conn.close()
        
        # ─────────────────────────────────────────────────────────────────────
        # RESULTADO FINAL
        # ─────────────────────────────────────────────────────────────────────
        print("\n" + "═" * 80)
        if validation_passed:
            print("✅ VALIDACIÓN EXITOSA: Todos los datos son consistentes y coherentes")
        else:
            print("⚠️  VALIDACIÓN COMPLETADA CON ERRORES: Revisar mensajes anteriores")
        print("═" * 80)
        
        return validation_passed
        
    except Exception as e:
        print(f"\n❌ ERROR durante la validación: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# SECCIÓN 10: FUNCIÓN PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    """
    Función principal que orquesta todo el proceso de generación y carga de datos
    """
    print("\n" + "═" * 80)
    print("  FLEETLOGIX - GENERADOR DE DATOS SINTÉTICOS")
    print("  Sistema de Gestión de Transporte y Logística")
    print("═" * 80)
    print(f"\n  📊 Configuración:")
    print(f"     • Vehículos: {NUM_VEHICLES:,}")
    print(f"     • Conductores: {NUM_DRIVERS:,}")
    print(f"     • Rutas: {NUM_ROUTES:,}")
    print(f"     • Viajes: {NUM_TRIPS:,}")
    print(f"     • Entregas: ~400,000")
    print(f"     • Mantenimientos: ~5,000")
    print(f"     • Total aproximado: ~505,650 registros")
    print(f"\n  🎲 Semilla aleatoria: {RANDOM_SEED} (reproducible)")
    print(f"  📅 Período operativo: 2024-2025 (2 años)")
    print("\n" + "═" * 80)
    
    # ─────────────────────────────────────────────────────────────────────────
    # PASO 1: Verificaciones Previas
    # ─────────────────────────────────────────────────────────────────────────
    print("\n🔍 PASO 1: Verificando requisitos previos...")
    
    if not verify_tables():
        print("\n❌ ERROR: Las tablas no existen. Ejecute fleetlogix_db_schema.sql primero.")
        sys.exit(1)
    
    # ─────────────────────────────────────────────────────────────────────────
    # PASO 2: Limpieza de Tablas
    # ─────────────────────────────────────────────────────────────────────────
    print("\n🔧 PASO 2: Preparando base de datos...")
    truncate_tables()
    
    # ─────────────────────────────────────────────────────────────────────────
    # PASO 3: Generación de Tablas Maestras
    # ─────────────────────────────────────────────────────────────────────────
    print("\n📋 PASO 3: Generando tablas maestras...")
    print("─" * 80)
    
    vehicles_df = generate_vehicles()
    drivers_df = generate_drivers()
    routes_df = generate_routes()
    
    print(f"\n✓ Tablas maestras generadas: {len(vehicles_df) + len(drivers_df) + len(routes_df):,} registros")
    
    # ─────────────────────────────────────────────────────────────────────────
    # PASO 4: Generación de Tablas Transaccionales
    # ─────────────────────────────────────────────────────────────────────────
    print("\n📊 PASO 4: Generando tablas transaccionales...")
    print("─" * 80)
    
    trips_df = generate_trips(vehicles_df, drivers_df, routes_df)
    deliveries_df = generate_deliveries(trips_df)
    maintenance_df = generate_maintenance(trips_df, vehicles_df)
    
    total_transactional = len(trips_df) + len(deliveries_df) + len(maintenance_df)
    print(f"\n✓ Tablas transaccionales generadas: {total_transactional:,} registros")
    
    # ─────────────────────────────────────────────────────────────────────────
    # PASO 5: Carga a Base de Datos
    # ─────────────────────────────────────────────────────────────────────────
    print("\n💾 PASO 5: Cargando datos a PostgreSQL...")
    print("─" * 80)
    
    load_data_to_table(vehicles_df, 'vehicles')
    load_data_to_table(drivers_df, 'drivers')
    load_data_to_table(routes_df, 'routes')
    load_data_to_table(trips_df, 'trips')
    load_data_to_table(deliveries_df, 'deliveries')
    load_data_to_table(maintenance_df, 'maintenance')
    
    total_records = len(vehicles_df) + len(drivers_df) + len(routes_df) + len(trips_df) + len(deliveries_df) + len(maintenance_df)
    print(f"\n✓ Total de registros cargados: {total_records:,}")
    
    # ─────────────────────────────────────────────────────────────────────────
    # PASO 6: Validación de Datos
    # ─────────────────────────────────────────────────────────────────────────
    print("\n✅ PASO 6: Validando consistencia y coherencia de datos...")
    print("─" * 80)
    
    validation_success = validate_data()
    
    # ─────────────────────────────────────────────────────────────────────────
    # RESUMEN FINAL
    # ─────────────────────────────────────────────────────────────────────────
    print("\n" + "═" * 80)
    print("  RESUMEN FINAL")
    print("═" * 80)
    
    summary_data = [
        ["Tabla", "Registros"],
        ["─" * 20, "─" * 15],
        ["vehicles", f"{len(vehicles_df):,}"],
        ["drivers", f"{len(drivers_df):,}"],
        ["routes", f"{len(routes_df):,}"],
        ["trips", f"{len(trips_df):,}"],
        ["deliveries", f"{len(deliveries_df):,}"],
        ["maintenance", f"{len(maintenance_df):,}"],
        ["─" * 20, "─" * 15],
        ["TOTAL", f"{total_records:,}"]
    ]
    
    print(tabulate(summary_data, headers="firstrow", tablefmt="simple"))
    
    print("\n" + "═" * 80)
    if validation_success:
        print("  ✅ PROCESO COMPLETADO EXITOSAMENTE")
        print("  Todos los datos han sido generados, cargados y validados correctamente")
    else:
        print("  ⚠️  PROCESO COMPLETADO CON ADVERTENCIAS")
        print("  Los datos fueron cargados pero hay inconsistencias detectadas")
    print("═" * 80 + "\n")


# ═══════════════════════════════════════════════════════════════════════════════
# PUNTO DE ENTRADA
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Proceso interrumpido por el usuario")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n❌ ERROR CRÍTICO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
