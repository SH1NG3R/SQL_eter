"""
Recolección de estadísticas de tablas
"""
from sqlalchemy import text
from typing import Dict, Any
from .database_connector import DatabaseConnector
from .logger_setup import LoggerSetup

class StatsCollector:
    """Recolecta estadísticas de tablas y base de datos"""
    
    def __init__(self, db_connector: DatabaseConnector):
        self.db_connector = db_connector
        self.engine = db_connector.get_engine()
        self.db_type = db_connector.db_type
        
        # Setup logger
        logger_setup = LoggerSetup()
        self.logger = logger_setup.setup_logger(self.__class__.__name__)
    
    def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """
        Obtiene estadísticas completas de una tabla
        
        Args:
            table_name: Nombre de la tabla
            
        Returns:
            Diccionario con estadísticas de la tabla
        """
        try:
            stats = {}
            
            with self.engine.connect() as conn:
                # Contar registros totales
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                stats['total_records'] = count_result.fetchone()[0]
                
                # Estadísticas específicas por tipo de BD
                if self.db_type == 'postgresql':
                    stats.update(self._get_postgresql_stats(conn, table_name))
                elif self.db_type == 'mysql':
                    stats.update(self._get_mysql_stats(conn, table_name))
                else:
                    stats['table_size'] = "N/A"
                    stats['index_size'] = "N/A"
            
            self.logger.info(f"Estadísticas recolectadas para {table_name}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Error obteniendo estadísticas: {str(e)}")
            return {
                "total_records": 0,
                "table_size": "Error",
                "index_size": "Error"
            }
    
    def _get_postgresql_stats(self, conn, table_name: str) -> Dict[str, Any]:
        """Estadísticas específicas de PostgreSQL"""
        stats = {}
        
        # Tamaño de tabla (por que el tamaño importa (?))
        size_result = conn.execute(
            text(f"SELECT pg_size_pretty(pg_total_relation_size('{table_name}'))")
        )
        stats['table_size'] = size_result.fetchone()[0]
        
        # Tamaño de índices (ya use el unico chiste que me sabia sobre el tamaño)
        index_size_result = conn.execute(
            text(f"SELECT pg_size_pretty(pg_indexes_size('{table_name}'))")
        )
        stats['index_size'] = index_size_result.fetchone()[0]
        
        return stats
    
    def _get_mysql_stats(self, conn, table_name: str) -> Dict[str, Any]:
        """Estadísticas específicas de MySQL"""
        stats = {}
        
        info_result = conn.execute(text(f"""
            SELECT 
                ROUND(((data_length + index_length) / 1024 / 1024), 2) AS table_size_mb,
                ROUND((index_length / 1024 / 1024), 2) AS index_size_mb
            FROM information_schema.TABLES 
            WHERE table_name = '{table_name}'
        """))
        
        result = info_result.fetchone()
        if result:
            stats['table_size'] = f"{result[0]} MB"
            stats['index_size'] = f"{result[1]} MB"
        else:
            stats['table_size'] = "N/A"
            stats['index_size'] = "N/A"
            
        return stats
    
    def compare_stats(self, before_stats: Dict[str, Any], 
                     after_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compara estadísticas antes y después de una operación
        """
        comparison = {
            'records_removed': before_stats['total_records'] - after_stats['total_records'],
            'before': before_stats,
            'after': after_stats
        }
        
        # Calcular porcentaje de reducción (solo si es posible)
        if before_stats['total_records'] > 0:
            reduction_pct = (comparison['records_removed'] / before_stats['total_records']) * 100
            comparison['reduction_percentage'] = round(reduction_pct, 2)
        
        return comparison