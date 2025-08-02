"""
Gestión de copias de seguridad
"""
from sqlalchemy import text
from datetime import datetime
from typing import Optional
from .database_connector import DatabaseConnector
from .logger_setup import LoggerSetup
from .config import DatabaseConfig

class BackupManager:
    """Maneja las copias de seguridad de tablas"""
    
    def __init__(self, db_connector: DatabaseConnector):
        self.db_connector = db_connector
        self.engine = db_connector.get_engine()
        
        # Setup logger
        logger_setup = LoggerSetup()
        self.logger = logger_setup.setup_logger(self.__class__.__name__)
    
    def create_backup(self, table_name: str, backup_suffix: Optional[str] = None) -> str:
        """
        Crea una copia de seguridad de la tabla
        
        Args:
            table_name: Nombre de la tabla original
            backup_suffix: Sufijo personalizado para el backup
            
        Returns:
            Nombre de la tabla de backup creada
        """
        if backup_suffix is None:
            backup_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        backup_name = f"{table_name}_{DatabaseConfig.BACKUP_PREFIX}_{backup_suffix}"
        
        try:
            with self.engine.connect() as conn:
                # Crear tabla de backup
                backup_query = f"CREATE TABLE {backup_name} AS SELECT * FROM {table_name}"
                conn.execute(text(backup_query))
                conn.commit()
                
            self.logger.info(f"Backup creado exitosamente: {backup_name}")
            return backup_name
            
        except Exception as e:
            self.logger.error(f"Error creando backup: {str(e)}")
            raise
    
    def verify_backup(self, original_table: str, backup_table: str) -> bool:
        """
        Verifica que el backup sea válido comparando conteos
        
        Args:
            original_table: Tabla original
            backup_table: Tabla de backup
            
        Returns:
            True si el backup es válido
        """
        try:
            with self.engine.connect() as conn:
                # Contar registros en tabla original
                original_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM {original_table}")
                ).fetchone()[0]
                
                # Contar registros en backup
                backup_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM {backup_table}")
                ).fetchone()[0]
                
            if original_count == backup_count:
                self.logger.info(f"Backup verificado: {backup_count} registros")
                return True
            else:
                self.logger.warning(
                    f"Backup inconsistente: Original={original_count}, Backup={backup_count}"
                )
                return False
                
        except Exception as e:
            self.logger.error(f"Error verificando backup: {str(e)}")
            return False
    
    def restore_from_backup(self, original_table: str, backup_table: str) -> bool:
        """
        Restaura una tabla desde su backup
        
        Args:
            original_table: Tabla a restaurar
            backup_table: Tabla de backup
            
        Returns:
            True si la restauración fue exitosa
        """
        try:
            with self.engine.connect() as conn:
                # Elimina la tabla original
                conn.execute(text(f"DROP TABLE IF EXISTS {original_table}"))
                
                # recrear desde el backup
                conn.execute(
                    text(f"CREATE TABLE {original_table} AS SELECT * FROM {backup_table}")
                )
                conn.commit()
                
            self.logger.info(f"Tabla {original_table} restaurada desde {backup_table}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error restaurando desde backup: {str(e)}")
            return False