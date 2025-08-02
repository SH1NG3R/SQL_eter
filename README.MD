# 🔧 Database Duplicate Repair System

Sistema modular y profesional para la detección, análisis y reparación automática de registros duplicados en bases de datos de producción.

# - IMPORTANTE -
Esta es una prueba de concepto y debe ser usada con precaucion, bajo su propio riesgo y siempre en entornos controlados. No posee ninguna garantia ni responsabilidad


## 📋 Características

- ✅ **Multi-BD**: Soporte para PostgreSQL, MySQL y SQLite
- ✅ **Seguro**: Sistema de backups automático antes de cualquier modificación
- ✅ **Flexible**: Múltiples estrategias de eliminación (mantener más antiguo/reciente)
- ✅ **Robusto**: Logging completo y manejo de errores
- ✅ **Modular**: Arquitectura separada por responsabilidades
- ✅ **Testeable**: Suite de tests unitarios incluida
- ✅ **CLI**: Interfaz de línea de comandos para automatización
- ✅ **Dry-run**: Modo simulación para validar antes de ejecutar

## 🏗️ Estructura del Proyecto

```
REPARA_TABLAS/
├── database_repair/          # Paquete principal
│   ├── __init__.py
│   ├── backup_manager.py     # Gestión de copias de seguridad
│   ├── config.py            # Configuraciones centralizadas
│   ├── database_connector.py # Conexiones a base de datos
│   ├── duplicate_analyzer.py # Análisis de duplicados
│   ├── duplicate_remover.py  # Eliminación de duplicados
│   ├── logger_setup.py      # Sistema de logging
│   ├── main.py              # Script principal interactivo
│   ├── stats_collector.py   # Recolección de estadísticas
│   └── table_compressor.py  # Compresión y optimización
├── support_utilities/        # Utilidades de soporte
│   ├── cli.py               # Interfaz de línea de comandos
│   ├── example_usage.py     # Ejemplos de uso
│   ├── setup.py             # Configuración de instalación
│   └── test_duplicate_repair.py # Tests unitarios
└── requirements.txt         # Dependencias del proyecto
```

## 🚀 Instalación

### 1. Clonar o descargar el proyecto
```bash
git clone <tu-repositorio>
cd REPARA_TABLAS
```

### 2. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 3. (Opcional) Instalar como paquete
```bash
pip install -e support_utilities/
```

## 📦 Dependencias

- **pandas** >= 1.3.0 - Manipulación de datos
- **sqlalchemy** >= 1.4.0 - ORM y conexiones BD
- **psycopg2-binary** >= 2.9.0 - Driver PostgreSQL
- **pymysql** >= 1.0.0 - Driver MySQL

## 🎯 Uso Rápido

### Ejemplo Básico


```python
from database_repair import DatabaseConnector, DuplicateRemover

# Conectar a la base de datos
connector = DatabaseConnector(
    "postgresql://user:password@localhost:5432/database", 
    "postgresql"
)

# Crear el reparador
remover = DuplicateRemover(connector)

# Simular eliminación de duplicados
result = remover.remove_duplicates_keep_oldest(
    table_name="usuarios",
    columns_to_check=["email", "nombre"],
    dry_run=True
)

print(f"Se eliminarían {result['deleted_count']} registros duplicados")

# Ejecutar eliminación real (después de confirmar)
result = remover.remove_duplicates_keep_oldest(
    table_name="usuarios", 
    columns_to_check=["email", "nombre"],
    dry_run=False
)
```

## 💻 Interfaces de Uso

### 1. Script Principal Interactivo
```bash
cd database_repair
python main.py
```
Interfaz paso a paso con menús interactivos.

### 2. Línea de Comandos (CLI)
```bash
cd support_utilities
python cli.py --db-type postgresql \
              --connection-string "postgresql://user:pass@localhost/db" \
              --table usuarios \
              --columns email nombre \
              --strategy oldest \
              --dry-run \
              --verbose
```

### 3. Uso 
```python
from database_repair import (
    DatabaseConnector, DuplicateAnalyzer, 
    DuplicateRemover, StatsCollector
)

# mas codigo acá
```

## ⚙️ Configuración

### Configurar Conexiones
Edita `database_repair/config.py`:

```python
CONNECTION_STRINGS = {
    'postgresql': 'postgresql://usuario:contraseña@localhost:5432/basedatos',
    'mysql': 'mysql+pymysql://usuario:contraseña@localhost:3306/basedatos',
    'sqlite': 'sqlite:///ruta/archivo.db'
}
```

### Personalizar Logging
```python
# En tu código
from database_repair.logger_setup import LoggerSetup

logger_setup = LoggerSetup(log_level='DEBUG')
logger = logger_setup.setup_logger('MiApp', 'mi_log.log')
```

## 🔍 Ejemplos de Uso

### Análisis Detallado
```python
from database_repair import DatabaseConnector, DuplicateAnalyzer, StatsCollector

connector = DatabaseConnector("sqlite:///test.db", "sqlite")
analyzer = DuplicateAnalyzer(connector)
stats = StatsCollector(connector)

# Obtener estadísticas iniciales
initial_stats = stats.get_table_stats("productos")
print(f"Registros totales: {initial_stats['total_records']}")

# Analizar duplicados
duplicates = analyzer.analyze_duplicates("productos", ["sku", "nombre"])
print(f"Grupos de duplicados: {len(duplicates)}")

# Ver primeros duplicados
print(duplicates.head())
```

### Procesamiento por Lotes
```python
tables_config = [
    ("usuarios", ["email"]),
    ("productos", ["sku", "nombre"]), 
    ("pedidos", ["numero_pedido"])
]

for table_name, columns in tables_config:
    print(f"\n--- Procesando {table_name} ---")
    
    # Analizar
    duplicates = analyzer.analyze_duplicates(table_name, columns)
    
    if len(duplicates) > 0:
        # Eliminar duplicados
        result = remover.remove_duplicates_keep_oldest(
            table_name, columns, dry_run=False
        )
        print(f"✅ Eliminados: {result['deleted_count']} duplicados")
    else:
        print("✅ No hay duplicados")
```

## 🧪 Testing

### Ejecutar Tests
```bash
cd support_utilities
python -m pytest test_duplicate_repair.py -v
```

### Ejecutar Tests Específicos
```bash
python -m pytest test_duplicate_repair.py::TestDuplicateAnalyzer -v
```

## 🛠️ API Referencia

### DatabaseConnector
```python
connector = DatabaseConnector(connection_string, db_type)
connector.test_connection()  # Verificar conexión
engine = connector.get_engine()  # Obtener SQLAlchemy engine
```

### DuplicateAnalyzer
```python
analyzer = DuplicateAnalyzer(db_connector)
duplicates_df = analyzer.analyze_duplicates(table_name, columns_list)
count = analyzer.count_total_duplicates(table_name, columns_list)
```

### DuplicateRemover
```python
remover = DuplicateRemover(db_connector)

# Mantener registro más antiguo
result = remover.remove_duplicates_keep_oldest(table, columns, dry_run=True)

# Mantener registro más reciente  
result = remover.remove_duplicates_keep_newest(table, columns, dry_run=True)
```

### BackupManager
```python
backup_mgr = BackupManager(db_connector)
backup_name = backup_mgr.create_backup("mi_tabla")
is_valid = backup_mgr.verify_backup("mi_tabla", backup_name)
restored = backup_mgr.restore_from_backup("mi_tabla", backup_name)
```

## ⚠️ Consideraciones Importantes

### Seguridad
- **Siempre usar `dry_run=True` primero** para verificar qué se eliminará
- **Los backups se crean automáticamente** antes de cualquier modificación
- **Verificar la conexión** antes de ejecutar operaciones masivas

### Rendimiento
- Para tablas muy grandes (>1M registros), considerar:
  - Procesar por lotes más pequeños
  - Ejecutar durante horarios de menor carga
  - Monitorear el espacio disponible en disco

### Monitoreo
- Todos los logs se guardan automáticamente con timestamp
- Verificar logs en caso de errores: `duplicate_repair_YYYYMMDD_HHMMSS.log`

## 🐛 Troubleshooting

### Error de Conexión
```
Error: No se pudo conectar a la base de datos
```
**Solución**: Verificar string de conexión, credenciales y que el servicio de BD esté activo.

### Error de Permisos
```
Error: permission denied for table
```
**Solución**: Asegurar que el usuario de BD tenga permisos SELECT, INSERT, DELETE en las tablas.

### Memoria Insuficiente
```
MemoryError: Unable to allocate array
```
**Solución**: Procesar tablas más pequeñas o usar procesamiento por lotes.


## 📄 Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo `LICENSE` para más detalles.

## 📞 Soporte

No hay soporte, es solo una prueba de concepto, pero si quieres puedes notificar por los canales regulares, pero no me gusta el spam.

---

