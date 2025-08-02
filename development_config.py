import os
from database_repair.config import DatabaseConfig

class DevelopmentConfig(DatabaseConfig):
    """Configuración específica para desarrollo"""
    
    # Override conexiones para desarrollo
    CONNECTION_STRINGS = {
        'postgresql': os.getenv(
            'DEV_POSTGRESQL_URL', 
            'postgresql://dev_user:dev_pass@localhost:5432/dev_database'
        ),
        'mysql': os.getenv(
            'DEV_MYSQL_URL',
            'mysql+pymysql://dev_user:dev_pass@localhost:3306/dev_database'
        ),
        'sqlite': os.getenv(
            'DEV_SQLITE_PATH',
            'sqlite:///dev_database.db'
        )
    }
    
    # Configuración de desarrollo
    LOG_LEVEL = 'DEBUG'
    BACKUP_PREFIX = 'dev_backup'
    
    # Configuraciones adicionales para desarrollo
    DRY_RUN_DEFAULT = True  # Siempre dry-run por defecto en desarrollo
    MAX_RECORDS_WITHOUT_CONFIRMATION = 100  # Pedir confirmación para más de 100 registros
    
    @classmethod
    def get_safe_connection_string(cls, db_type: str) -> str:
        """Obtiene string de conexión seguro para desarrollo"""
        base_string = cls.get_connection_string(db_type)
        
        # Para desarrollo, siempre usar una base de datos de prueba
        if 'localhost' not in base_string and 'dev' not in base_string:
            print("⚠️  ADVERTENCIA: No parece ser una configuración de desarrollo")
            confirm = input("¿Continuar? (y/N): ")
            if confirm.lower() != 'y':
                raise ValueError("Operación cancelada por seguridad")
        
        return base_string
