"""
Manejo de conexiones a base de datos
"""
from sqlalchemy import create_engine, Engine
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional
from .logger_setup import LoggerSetup
from .config import DatabaseConfig

class DatabaseConnector:
    """Maneja las conexiones a diferentes tipos de base de datos"""
    
    def __init__(self, connection_string: str, db_type: str):
        self.connection_string = connection_string
        self.db_type = db_type
        self.engine: Optional[Engine] = None
        
        # Setup logger
        logger_setup = LoggerSetup()
        self.logger = logger_setup.setup_logger(self.__class__.__name__)
        
        self._validate_db_type()
        self._create_engine()
    
    def _validate_db_type(self):
        """Valida que el tipo de BD sea soportado"""
        if self.db_type not in DatabaseConfig.SUPPORTED_DB_TYPES:
            raise ValueError(f"Tipo de BD no soportado: {self.db_type}")
    
    def _create_engine(self):
        """Crea el engine de SQLAlchemy"""
        try:
            self.engine = create_engine(self.connection_string)
            self.logger.info(f"Engine creado para {self.db_type}")
        except SQLAlchemyError as e:
            self.logger.error(f"Error creando engine: {str(e)}")
            raise
    
    def get_engine(self) -> Engine:
        """Retorna el engine de la base de datos"""
        if self.engine is None:
            raise RuntimeError("Engine no inicializado")
        return self.engine
    
    def test_connection(self) -> bool:
        """Prueba la conexión a la base de datos"""
        try:
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")
            self.logger.info("Conexión exitosa")
            return True
        except Exception as e:
            self.logger.error(f"Error de conexión: {str(e)}")
            return False