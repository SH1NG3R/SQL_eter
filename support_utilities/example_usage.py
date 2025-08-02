"""
Ejemplos de uso del sistema modular
"""
import sys
import os


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from database_repair import (
    DatabaseConnector, DuplicateAnalyzer, DuplicateRemover,
    TableCompressor, StatsCollector
)

def example_basic_usage():
    """Ejemplo básico de uso"""
    print("=== EJEMPLO BÁSICO ===")
    
    # Conectar
    connector = DatabaseConnector(
        "postgresql://user:pass@localhost/db", 
        "postgresql"
    )
    
    # Analizar
    analyzer = DuplicateAnalyzer(connector)
    duplicates = analyzer.analyze_duplicates("users", ["email"])
    
    # Eliminar (simulacro)
    remover = DuplicateRemover(connector)
    result = remover.remove_duplicates_keep_oldest(
        "users", ["email"], dry_run=True
    )
    
    print(f"Se eliminarían {result['deleted_count']} duplicados")

def example_custom_workflow():
    """Ejemplo de flujo personalizado"""
    print("\n=== EJEMPLO FLUJO PERSONALIZADO ===")
    
    connector = DatabaseConnector("sqlite:///test.db", "sqlite")
    
    # Componentes
    analyzer = DuplicateAnalyzer(connector)
    remover = DuplicateRemover(connector)
    stats = StatsCollector(connector)
    compressor = TableCompressor(connector)
    
    # Workflow personalizado
    tables_to_clean = ["users", "products", "orders"]
    
    for table in tables_to_clean:
        print(f"\n--- Procesando {table} ---")
        
        try:
            # Estadísticas iniciales
            initial = stats.get_table_stats(table)
            print(f"Registros iniciales: {initial['total_records']}")
            
            # Buscar duplicados (personalizar columnas según caso)
            duplicate_columns = ["id", "name"]  # Ejemplo - ajustar según tabla
            
            duplicates = analyzer.analyze_duplicates(table, duplicate_columns)
            
            if len(duplicates) > 0:
                # Eliminar duplicados
                result = remover.remove_duplicates_keep_oldest(
                    table, duplicate_columns, dry_run=False
                )
                print(f"Eliminados: {result['deleted_count']} duplicados")
                
                # Comprimir
                compressor.compress_table(table)
                
                # Estadísticas finales
                final = stats.get_table_stats(table)
                comparison = stats.compare_stats(initial, final)
                print(f"Reducción: {comparison.get('reduction_percentage', 0):.2f}%")
            else:
                print("No hay duplicados")
                
        except Exception as e:
            print(f"Error procesando {table}: {str(e)}")

def example_batch_processing():
    """Ejemplo de procesamiento por lotes"""
    print("\n=== EJEMPLO PROCESAMIENTO POR LOTES ===")
    
    from concurrent.futures import ThreadPoolExecutor
    import time
    
    def process_table(table_config):
        """Procesa una tabla específica"""
        table_name, columns, connection_string, db_type = table_config
        
        try:
            connector = DatabaseConnector(connection_string, db_type)
            remover = DuplicateRemover(connector)
            
            result = remover.remove_duplicates_keep_oldest(
                table_name, columns, dry_run=False
            )
            
            return {
                'table': table_name,
                'deleted': result['deleted_count'],
                'status': 'success'
            }
            
        except Exception as e:
            return {
                'table': table_name,
                'error': str(e),
                'status': 'error'
            }
    
    # Configuración de tablas a procesar
    tables_config = [
        ("users", ["email"], "postgresql://user:pass@localhost/db1", "postgresql"),
        ("products", ["sku", "name"], "postgresql://user:pass@localhost/db2", "postgresql"),
        ("orders", ["order_id"], "mysql+pymysql://user:pass@localhost/db3", "mysql"),
    ]
    
    print("Iniciando procesamiento por lotes...")
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(process_table, tables_config))
    
    end_time = time.time()
    
    # resultados
    print(f"\nProcesamiento completado en {end_time - start_time:.2f} segundos")
    for result in results:
        if result['status'] == 'success':
            print(f"✅ {result['table']}: {result['deleted']} duplicados eliminados")
        else:
            print(f"❌ {result['table']}: Error - {result['error']}")

def example_with_specific_database():
    """Ejemplo con configuración específica de base de datos"""
    print("\n=== EJEMPLO CON BD ESPECÍFICA ===")
    
    # Configuraciones de ejemplo para diferentes tipos de BD
    database_configs = {
        'postgresql': {
            'connection_string': 'postgresql://username:password@localhost:5432/database_name',
            'db_type': 'postgresql'
        },
        'mysql': {
            'connection_string': 'mysql+pymysql://username:password@localhost:3306/database_name',
            'db_type': 'mysql'
        },
        'sqlite': {
            'connection_string': 'sqlite:///database.db',
            'db_type': 'sqlite'
        }
    }
    
    # Seleccionar configuración
    selected_db = 'sqlite'  # Cambiar según necesites
    config = database_configs[selected_db]
    
    try:
        # Conectar
        connector = DatabaseConnector(
            config['connection_string'], 
            config['db_type']
        )
        
        if not connector.test_connection():
            print(f"❌ No se pudo conectar a {selected_db}")
            return
        
        print(f"✅ Conectado exitosamente a {selected_db}")
        
        # Usar todos los componentes
        analyzer = DuplicateAnalyzer(connector)
        remover = DuplicateRemover(connector)
        stats = StatsCollector(connector)
        compressor = TableCompressor(connector)
        
        # Ejemplo completo
        table_name = "ejemplo_tabla"
        columns = ["email", "nombre"]
        
        print(f"\nProcesando tabla: {table_name}")
        print(f"Columnas de duplicados: {columns}")
        
        # 1. Estadísticas iniciales
        initial_stats = stats.get_table_stats(table_name)
        print(f"Registros iniciales: {initial_stats['total_records']}")
        
        # 2. Análisis
        duplicates = analyzer.analyze_duplicates(table_name, columns)
        print(f"Grupos de duplicados encontrados: {len(duplicates)}")
        
        # 3. Simulacro
        dry_result = remover.remove_duplicates_keep_oldest(table_name, columns, dry_run=True)
        print(f"Se eliminarían: {dry_result['deleted_count']} registros")
        
        # 4. Ejecución real (comentado para nuestra seguridad xD)
        # real_result = remover.remove_duplicates_keep_oldest(table_name, columns, dry_run=False)
        # print(f"Eliminados: {real_result['deleted_count']} registros")
        
        # 5. Compresión
        # compressor.compress_table(table_name)
        
    except Exception as e:
        print(f"❌ Error en ejemplo: {str(e)}")

def main():
    """Ejecutar todos los ejemplos"""
    print("🚀 EJECUTANDO EJEMPLOS DE USO")
    print("=" * 50)
    
    try:
        example_basic_usage()
        example_custom_workflow() 
        example_batch_processing()
        example_with_specific_database()
        
        print("\n" + "=" * 50)
        print("✅ Todos los ejemplos ejecutados")
        print("\nNOTA: Algunos ejemplos están en modo simulación")
        print("      Ajusta las configuraciones de BD antes de usar en producción")
        
    except Exception as e:
        print(f"\n❌ Error ejecutando ejemplos: {str(e)}")

if __name__ == "__main__":
    main()