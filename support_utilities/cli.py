"""
Interfaz de línea de comandos
"""
import argparse
import sys
import os

# Agregar el directorio padre al path para importar database_repair
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from database_repair import DatabaseConnector, DuplicateRemover, StatsCollector
from database_repair.config import DatabaseConfig

def create_parser():
    """Crear parser de argumentos"""
    parser = argparse.ArgumentParser(
        description="Herramienta para reparación de duplicados en BD"
    )
    
    parser.add_argument(
        '--db-type', 
        choices=['postgresql', 'mysql', 'sqlite'],
        required=True,
        help='Tipo de base de datos'
    )
    
    parser.add_argument(
        '--connection-string',
        required=True,
        help='String de conexión a la BD'
    )
    
    parser.add_argument(
        '--table',
        required=True,
        help='Nombre de la tabla a procesar'
    )
    
    parser.add_argument(
        '--columns',
        required=True,
        nargs='+',
        help='Columnas que definen duplicados'
    )
    
    parser.add_argument(
        '--strategy',
        choices=['oldest', 'newest'],
        default='oldest',
        help='Estrategia: mantener registro más antiguo o más nuevo'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Solo simular, no ejecutar cambios'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Mostrar información detallada'
    )
    
    return parser

def main_cli():
    """Función principal CLI"""
    parser = create_parser()
    args = parser.parse_args()
    
    try:
        # Conectar
        connector = DatabaseConnector(args.connection_string, args.db_type)
        
        if not connector.test_connection():
            print("❌ Error: No se pudo conectar a la base de datos")
            sys.exit(1)
        
        # Componentes
        remover = DuplicateRemover(connector)
        stats_collector = StatsCollector(connector)
        
        if args.verbose:
            print(f"📊 Obteniendo estadísticas de {args.table}...")
            initial_stats = stats_collector.get_table_stats(args.table)
            print(f"Registros iniciales: {initial_stats['total_records']}")
        
        # Ejecutar reparación
        method = (remover.remove_duplicates_keep_oldest 
                 if args.strategy == 'oldest' 
                 else remover.remove_duplicates_keep_newest)
        
        result = method(args.table, args.columns, args.dry_run)
        
        # Mostrar resultados
        if args.dry_run:
            print(f"🔍 SIMULACRO: Se eliminarían {result['deleted_count']} duplicados")
        else:
            print(f"✅ Eliminados {result['deleted_count']} duplicados")
            if result.get('backup_table'):
                print(f"💾 Backup creado: {result['backup_table']}")
        
        if args.verbose and not args.dry_run and result['deleted_count'] > 0:
            final_stats = stats_collector.get_table_stats(args.table)
            print(f"Registros finales: {final_stats['total_records']}")
    
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main_cli()