"""
Eliminación de registros duplicados
"""
from sqlalchemy import text
from typing import List, Dict, Any
from .database_connector import DatabaseConnector
from .backup_manager import BackupManager
from .duplicate_analyzer import DuplicateAnalyzer
from .logger_setup import LoggerSetup

class DuplicateRemover:
    """Elimina registros duplicados de las tablas"""
    
    def __init__(self, db_connector: DatabaseConnector):
        self.db_connector = db_connector
        self.engine = db_connector.get_engine()
        self.db_type = db_connector.db_type
        self.backup_manager = BackupManager(db_connector)
        self.analyzer = DuplicateAnalyzer(db_connector)
        logger_setup = LoggerSetup()
        self.logger = logger_setup.setup_logger(self.__class__.__name__)
    
    def remove_duplicates_keep_oldest(self, table_name: str, columns_to_check: List[str], 
                                    dry_run: bool = True) -> Dict[str, Any]:
        """
        Elimina duplicados manteniendo el registro más antiguo (menor ID)
        """
        return self._remove_duplicates(table_name, columns_to_check, 'MIN', dry_run)
    
    def remove_duplicates_keep_newest(self, table_name: str, columns_to_check: List[str], 
                                    dry_run: bool = True) -> Dict[str, Any]:
        """
        Elimina duplicados manteniendo el registro más reciente (mayor ID)
        """
        return self._remove_duplicates(table_name, columns_to_check, 'MAX', dry_run)
    
    def _remove_duplicates(self, table_name: str, columns_to_check: List[str], 
                          keep_strategy: str, dry_run: bool = True) -> Dict[str, Any]:
        """
        Método base para eliminar duplicados
        
        Args:
            table_name: Nombre de la tabla
            columns_to_check: Columnas que definen duplicado
            keep_strategy: 'MIN' para más antiguo, 'MAX' para más reciente
            dry_run: Si True, solo simula la operación
        """
        try:
            # Verificar si hay duplicados
            duplicates_df = self.analyzer.analyze_duplicates(table_name, columns_to_check)
            
            if duplicates_df.empty:
                return {
                    "status": "success", 
                    "deleted_count": 0, 
                    "message": "No hay duplicados"
                }
            
            # Crear backup si no es dry_run
            backup_name = None
            if not dry_run:
                backup_name = self.backup_manager.create_backup(table_name)
            
            # Construir query de eliminación
            columns_str = ', '.join(columns_to_check)
            delete_query = self._build_delete_query(table_name, columns_str, keep_strategy)
            
            deleted_count = 0
            
            if dry_run:
                deleted_count = self._count_records_to_delete(table_name, columns_str, keep_strategy)
                self.logger.info(f"DRY RUN: Se eliminarían {deleted_count} registros duplicados")
            else:
                deleted_count = self._execute_deletion(delete_query)
                self.logger.info(f"Eliminados {deleted_count} registros duplicados")
            
            return {
                "status": "success",
                "deleted_count": deleted_count,
                "backup_table": backup_name,
                "dry_run": dry_run,
                "strategy": "oldest" if keep_strategy == "MIN" else "newest"
            }
            
        except Exception as e:
            self.logger.error(f"Error eliminando duplicados: {str(e)}")
            raise
    
    def _build_delete_query(self, table_name: str, columns_str: str, keep_strategy: str) -> str:
        """Construye la query de eliminación según el tipo de BD"""
        base_query = f"""
        DELETE FROM {table_name}
        WHERE id NOT IN (
            SELECT {keep_strategy}(id)
            FROM {table_name}
            GROUP BY {columns_str}
        )
        """
        
        # MySQL necesita subconsulta adicional
        if self.db_type == 'mysql':
            base_query = f"""
            DELETE FROM {table_name}
            WHERE id NOT IN (
                SELECT * FROM (
                    SELECT {keep_strategy}(id)
                    FROM {table_name}
                    GROUP BY {columns_str}
                ) as temp
            )
            """
        
        return base_query
    
    def _count_records_to_delete(self, table_name: str, columns_str: str, keep_strategy: str) -> int:
        """Cuenta registros que serían eliminados"""
        count_query = f"""
        SELECT COUNT(*) as count
        FROM {table_name}
        WHERE id NOT IN (
            SELECT {keep_strategy}(id)
            FROM {table_name}
            GROUP BY {columns_str}
        )
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(count_query))
            return result.fetchone()[0]
    
    def _execute_deletion(self, delete_query: str) -> int:
        """Ejecuta la eliminación real"""
        with self.engine.connect() as conn:
            result = conn.execute(text(delete_query))
            conn.commit()
            return result.rowcount
