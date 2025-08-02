"""
Compresión y optimización de tablas
"""
from sqlalchemy import text
from .database_connector import DatabaseConnector
from .logger_setup import LoggerSetup

class TableCompressor:
    """Comprime y optimiza tablas después de la limpieza"""
    
    def __init__(self, db_connector: DatabaseConnector):
        self.db_connector = db_connector
        self.engine = db_connector.get_engine()
        self.db_type = db_connector.db_type
        
        # Setup logger
        logger_setup = LoggerSetup()
        self.logger = logger_setup.setup_logger(self.__class__.__name__)
    
    def compress_table(self, table_name: str) -> bool:
        """
        Comprime la tabla según el tipo de base de datos
        
        Args:
            table_name: Nombre de la tabla a comprimir
            
        Returns:
            True si la compresión fue exitosa
        """
        try:
            with self.engine.connect() as conn:
                if self.db_type == 'postgresql':
                    conn.execute(text(f"VACUUM ANALYZE {table_name}"))
                elif self.db_type == 'mysql':
                    conn.execute(text(f"OPTIMIZE TABLE {table_name}"))
                elif self.db_type == 'sqlite':
                    conn.execute(text("VACUUM"))
                
                conn.commit()
                
            self.logger.info(f"Tabla {table_name} comprimida exitosamente")
            return True
            
        except Exception as e:
            self.logger.error(f"Error comprimiendo tabla: {str(e)}")
            return False
    
    def analyze_table_stats(self, table_name: str) -> bool:
        """Actualiza estadísticas de la tabla (PostgreSQL)"""
        if self.db_type != 'postgresql':
            self.logger.info("Análisis de estadísticas solo disponible para PostgreSQL")
            return True
            
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"ANALYZE {table_name}"))
                conn.commit()
                
            self.logger.info(f"Estadísticas actualizadas para {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error actualizando estadísticas: {str(e)}")
            return False
    
    def reindex_table(self, table_name: str) -> bool:
        """Reindexar tabla (PostgreSQL)"""
        if self.db_type != 'postgresql':
            self.logger.info("Reindexación solo disponible para PostgreSQL")
            return True
            
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"REINDEX TABLE {table_name}"))
                conn.commit()
                
            self.logger.info(f"Tabla {table_name} reindexada")
            return True
            
        except Exception as e:
            self.logger.error(f"Error reindexando tabla: {str(e)}")
            return False