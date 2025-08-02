"""
Configuración del sistema de logging
"""
import logging
from datetime import datetime
from typing import Optional

class LoggerSetup:
    """Manager para configurar el logging del sistema"""
    
    def __init__(self, log_level: str = 'INFO'):
        self.log_level = getattr(logging, log_level.upper())
        self.logger = None
    
    def setup_logger(self, name: str, log_file: Optional[str] = None) -> logging.Logger:
        """
        Configura y retorna un logger
        
        Args:
            name: Nombre del logger
            log_file: Archivo de log opcional
            
        Returns:
            Logger configurado
        """
        logger = logging.getLogger(name)
        
        if logger.handlers:
            return logger
            
        logger.setLevel(self.log_level)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler (si se especifica)
        if log_file is None:
            log_file = f'duplicate_repair_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
            
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        return logger