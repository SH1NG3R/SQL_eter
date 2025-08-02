"""
Database Duplicate Repair Package
"""
from .database_connector import DatabaseConnector
from .duplicate_analyzer import DuplicateAnalyzer
from .duplicate_remover import DuplicateRemover
from .table_compressor import TableCompressor
from .backup_manager import BackupManager
from .stats_collector import StatsCollector

__version__ = "1.0.0"
__all__ = [
    'DatabaseConnector',
    'DuplicateAnalyzer', 
    'DuplicateRemover',
    'TableCompressor',
    'BackupManager',
    'StatsCollector'
]