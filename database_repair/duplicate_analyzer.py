"""
Análisis de registros duplicados
"""
import pandas as pd
from sqlalchemy import text
from typing import List
from .database_connector import DatabaseConnector
from .logger_setup import LoggerSetup

class DuplicateAnalyzer:
    """Analiza y encuentra registros duplicados en tablas"""
    
    def __init__(self, db_connector: DatabaseConnector):
        self.db_connector = db_connector
        self.engine = db_connector.get_engine()
        self.db_type = db_connector.db_type
        
        # Setup logger
        logger_setup = LoggerSetup()
        self.logger = logger_setup.setup_logger(self.__class__.__name__)
    
    def analyze_duplicates(self, table_name: str, columns_to_check: List[str]) -> pd.DataFrame:
        """
        Analiza duplicados en una tabla específica
        
        Args:
            table_name: Nombre de la tabla
            columns_to_check: Columnas que definen un duplicado
            
        Returns:
            DataFrame con información sobre duplicados
        """
        try:
            columns_str = ', '.join(columns_to_check)
            
            # Query base
            query = f"""
            WITH duplicates AS (
                SELECT {columns_str}, COUNT(*) as duplicate_count,
                       MIN(id) as min_id, MAX(id) as max_id,
                       ARRAY_AGG(id ORDER BY id) as all_ids
                FROM {table_name}
                GROUP BY {columns_str}
                HAVING COUNT(*) > 1
            )
            SELECT * FROM duplicates
            ORDER BY duplicate_count DESC
            """
            
            # Ajustar query según tipo de BD
            query = self._adjust_query_for_db_type(query)
            
            duplicates_df = pd.read_sql(query, self.engine)
            
            self.logger.info(f"Encontrados {len(duplicates_df)} grupos de duplicados en {table_name}")
            if len(duplicates_df) > 0:
                total_duplicates = duplicates_df['duplicate_count'].sum() - len(duplicates_df)
                self.logger.info(f"Total de registros duplicados: {total_duplicates}")
            
            return duplicates_df
            
        except Exception as e:
            self.logger.error(f"Error analizando duplicados: {str(e)}")
            raise
    
    def _adjust_query_for_db_type(self, query: str) -> str:
        """Ajusta la query según el tipo de base de datos"""
        if self.db_type == 'mysql':
            query = query.replace('ARRAY_AGG(id ORDER BY id)', 'GROUP_CONCAT(id ORDER BY id)')
        elif self.db_type == 'sqlite':
            query = query.replace('ARRAY_AGG(id ORDER BY id)', 'GROUP_CONCAT(id)')
        
        return query
    
    def count_total_duplicates(self, table_name: str, columns_to_check: List[str]) -> int:
        """Cuenta el total de registros duplicados"""
        try:
            columns_str = ', '.join(columns_to_check)
            
            count_query = f"""
            SELECT COUNT(*) - COUNT(DISTINCT {columns_str}) as duplicate_count
            FROM {table_name}
            """
            
            with self.engine.connect() as conn:
                result = conn.execute(text(count_query))
                count = result.fetchone()[0]
            
            return count
            
        except Exception as e:
            self.logger.error(f"Error contando duplicados: {str(e)}")
            return 0