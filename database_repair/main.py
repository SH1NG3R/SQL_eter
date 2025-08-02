"""
Script principal para ejecutar la reparación de duplicados
"""
from database_repair import (
    DatabaseConnector, DuplicateAnalyzer, DuplicateRemover,
    TableCompressor, BackupManager, StatsCollector
)
from database_repair.config import DatabaseConfig

def main():
    """Función principal"""
    
    # Configuración
    DB_TYPE = 'postgresql'  # Cambiar según tu BD
    CONNECTION_STRING = DatabaseConfig.get_connection_string(DB_TYPE)
    TABLE_NAME = "mi_tabla_problema"
    DUPLICATE_COLUMNS = ['email', 'nombre', 'fecha_creacion']
    
    try:
        # 1. Conectar a la base de datos
        print("=== CONECTANDO A LA BASE DE DATOS ===")
        db_connector = DatabaseConnector(CONNECTION_STRING, DB_TYPE)
        
        if not db_connector.test_connection():
            print("Error: No se pudo conectar a la base de datos")
            return
        
        # 2. Inicializar componentes
        analyzer = DuplicateAnalyzer(db_connector)
        remover = DuplicateRemover(db_connector)
        compressor = TableCompressor(db_connector)
        stats_collector = StatsCollector(db_connector)
        
        # 3. Estadísticas iniciales
        print("\n=== ESTADÍSTICAS INICIALES ===")
        initial_stats = stats_collector.get_table_stats(TABLE_NAME)
        print(f"Registros totales: {initial_stats['total_records']}")
        print(f"Tamaño de tabla: {initial_stats['table_size']}")
        
        # 4. Analizar duplicados
        print("\n=== ANÁLISIS DE DUPLICADOS ===")
        duplicates = analyzer.analyze_duplicates(TABLE_NAME, DUPLICATE_COLUMNS)
        
        if len(duplicates) == 0:
            print("No se encontraron duplicados")
            return
        
        print(f"Grupos de duplicados: {len(duplicates)}")
        print("\nPrimeros 5 grupos:")
        print(duplicates.head())
        
        # 5. Simulacro de eliminación
        print("\n=== SIMULACRO DE ELIMINACIÓN ===")
        dry_result = remover.remove_duplicates_keep_oldest(
            TABLE_NAME, DUPLICATE_COLUMNS, dry_run=True
        )
        print(f"Se eliminarían: {dry_result['deleted_count']} registros")
        
        # 6. Confirmar eliminación real
        confirm = input("\n¿Proceder con la eliminación real? (y/N): ")
        if confirm.lower() != 'y':
            print("Operación cancelada")
            return
        
        # 7. Eliminación real
        print("\n=== ELIMINACIÓN REAL ===")
        real_result = remover.remove_duplicates_keep_oldest(
            TABLE_NAME, DUPLICATE_COLUMNS, dry_run=False
        )
        print(f"Eliminados: {real_result['deleted_count']} registros")
        print(f"Backup creado: {real_result['backup_table']}")
        
        # 8. Comprimir tabla
        print("\n=== COMPRIMIENDO TABLA ===")
        compressor.compress_table(TABLE_NAME)
        
        # 9. Estadísticas finales
        print("\n=== ESTADÍSTICAS FINALES ===")
        final_stats = stats_collector.get_table_stats(TABLE_NAME)
        print(f"Registros totales: {final_stats['total_records']}")
        print(f"Tamaño de tabla: {final_stats['table_size']}")
        
        # 10. Comparación
        comparison = stats_collector.compare_stats(initial_stats, final_stats)
        print(f"\n=== RESUMEN DE LA OPERACIÓN ===")
        print(f"Registros eliminados: {comparison['records_removed']}")
        if 'reduction_percentage' in comparison:
            print(f"Reducción: {comparison['reduction_percentage']}%")
        
        print("\n✅ Reparación completada exitosamente")
        
    except Exception as e:
        print(f"❌ Error durante la reparación: {str(e)}")

if __name__ == "__main__":
    main()
