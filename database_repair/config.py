"""
Configuración general del sistema
"""
from typing import Dict

class DatabaseConfig:
    """Configuraciones de base de datos"""
    
    CONNECTION_STRINGS = {
        'postgresql': 'postgresql://username:password@localhost:5432/database_name',
        'mysql': 'mysql+pymysql://username:password@localhost:3306/database_name',
        'sqlite': 'sqlite:///database.db'
    }
    
    SUPPORTED_DB_TYPES = ['postgresql', 'mysql', 'sqlite']
    
    # Config logging
    LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    LOG_LEVEL = 'INFO'
    
    # Config de backup
    BACKUP_PREFIX = 'backup'
    
    @classmethod
    def get_connection_string(cls, db_type: str, **kwargs) -> str:
        """Obtiene string de conexión personalizado"""
        if db_type not in cls.SUPPORTED_DB_TYPES:
            raise ValueError(f"Tipo de BD no soportado: {db_type}")
        
        base_string = cls.CONNECTION_STRINGS[db_type]
        
        # Reemplaza aqui los parámetros 
        for key, value in kwargs.items():
            base_string = base_string.replace(key, str(value))
            
        return base_string