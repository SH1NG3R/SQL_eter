"""
Tests unitarios para el sistema
"""
import unittest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from database_repair import (
    DatabaseConnector, DuplicateAnalyzer, 
    DuplicateRemover, StatsCollector, BackupManager
)

class TestDatabaseConnector(unittest.TestCase):
    """Tests para DatabaseConnector"""
    
    def test_invalid_db_type(self):
        """Test que falla con tipo de BD inválido"""
        with self.assertRaises(ValueError):
            DatabaseConnector("invalid://connection", "invalid_db")
    
    @patch('database_repair.database_connector.create_engine')
    def test_engine_creation(self, mock_create_engine):
        """Test creación de engine"""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        connector = DatabaseConnector("sqlite:///test.db", "sqlite")
        
        self.assertEqual(connector.get_engine(), mock_engine)
        mock_create_engine.assert_called_once()
    
    def test_supported_db_types(self):
        """Test tipos de BD soportados"""
        supported_types = ['postgresql', 'mysql', 'sqlite']
        
        for db_type in supported_types:
            # No debería lanzar excepción para tipos soportados
            try:
                # Mock create_engine para evitar conexión real
                with patch('database_repair.database_connector.create_engine'):
                    connector = DatabaseConnector(f"{db_type}://test", db_type)
                    self.assertEqual(connector.db_type, db_type)
            except ValueError:
                self.fail(f"Tipo de BD soportado {db_type} causó ValueError")

class TestDuplicateAnalyzer(unittest.TestCase):
    """Tests para DuplicateAnalyzer"""
    
    def setUp(self):
        self.mock_connector = Mock()
        self.mock_engine = Mock()
        self.mock_connector.get_engine.return_value = self.mock_engine
        self.mock_connector.db_type = 'postgresql'
        
        self.analyzer = DuplicateAnalyzer(self.mock_connector)
    
    @patch('database_repair.duplicate_analyzer.pd.read_sql')
    def test_analyze_duplicates_with_results(self, mock_read_sql):
        """Test análisis de duplicados con resultados"""
        # Mock DataFrame resultado
        mock_df = pd.DataFrame({
            'email': ['test@test.com', 'otro@test.com'],
            'duplicate_count': [2, 3],
            'min_id': [1, 10],
            'max_id': [2, 12],
            'all_ids': ['{1,2}', '{10,11,12}']
        })
        mock_read_sql.return_value = mock_df
        
        result = self.analyzer.analyze_duplicates('users', ['email'])
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]['duplicate_count'], 2)
        self.assertEqual(result.iloc[1]['duplicate_count'], 3)
    
    @patch('database_repair.duplicate_analyzer.pd.read_sql')
    def test_analyze_duplicates_no_results(self, mock_read_sql):
        """Test análisis sin duplicados"""
        mock_read_sql.return_value = pd.DataFrame()
        
        result = self.analyzer.analyze_duplicates('users', ['email'])
        
        self.assertTrue(result.empty)
    
    def test_adjust_query_for_mysql(self):
        """Test ajuste de query para MySQL"""
        self.analyzer.db_type = 'mysql'
        
        original_query = "SELECT ARRAY_AGG(id ORDER BY id) FROM table"
        adjusted_query = self.analyzer._adjust_query_for_db_type(original_query)
        
        self.assertIn('GROUP_CONCAT(id ORDER BY id)', adjusted_query)
        self.assertNotIn('ARRAY_AGG', adjusted_query)
    
    def test_adjust_query_for_sqlite(self):
        """Test ajuste de query para SQLite"""
        self.analyzer.db_type = 'sqlite'
        
        original_query = "SELECT ARRAY_AGG(id ORDER BY id) FROM table"
        adjusted_query = self.analyzer._adjust_query_for_db_type(original_query)
        
        self.assertIn('GROUP_CONCAT(id)', adjusted_query)
        self.assertNotIn('ARRAY_AGG', adjusted_query)

class TestDuplicateRemover(unittest.TestCase):
    """Tests para DuplicateRemover"""
    
    def setUp(self):
        self.mock_connector = Mock()
        self.mock_engine = Mock()
        self.mock_connector.get_engine.return_value = self.mock_engine
        self.mock_connector.db_type = 'postgresql'
        
        self.remover = DuplicateRemover(self.mock_connector)
    
    def test_dry_run_no_duplicates(self):
        """Test modo dry run sin duplicados"""
        # Mock analyzer para que no encuentre duplicados
        self.remover.analyzer = Mock()
        self.remover.analyzer.analyze_duplicates.return_value = pd.DataFrame()
        
        result = self.remover.remove_duplicates_keep_oldest(
            'users', ['email'], dry_run=True
        )
        
        self.assertEqual(result['deleted_count'], 0)
        self.assertEqual(result['message'], "No hay duplicados")
        self.assertTrue(result['dry_run'])
        self.assertEqual(result['status'], 'success')
    
    def test_strategy_parameter(self):
        """Test parámetro de estrategia"""
        # Mock analyzer con duplicados
        mock_df = pd.DataFrame({'email': ['test@test.com'], 'duplicate_count': [2]})
        self.remover.analyzer = Mock()
        self.remover.analyzer.analyze_duplicates.return_value = mock_df
        
        # Mock count method
        self.remover._count_records_to_delete = Mock(return_value=5)
        
        # Test estrategia oldest
        result_oldest = self.remover.remove_duplicates_keep_oldest(
            'users', ['email'], dry_run=True
        )
        self.assertEqual(result_oldest['strategy'], 'oldest')
        
        # Test estrategia newest  
        result_newest = self.remover.remove_duplicates_keep_newest(
            'users', ['email'], dry_run=True
        )
        self.assertEqual(result_newest['strategy'], 'newest')
    
    def test_build_delete_query_postgresql(self):
        """Test construcción de query para PostgreSQL"""
        self.remover.db_type = 'postgresql'
        
        query = self.remover._build_delete_query('users', 'email, name', 'MIN')
        
        self.assertIn('DELETE FROM users', query)
        self.assertIn('MIN(id)', query)
        self.assertIn('GROUP BY email, name', query)
        # PostgreSQL no necesita subconsulta extra
        self.assertNotIn('SELECT * FROM (', query)
    
    def test_build_delete_query_mysql(self):
        """Test construcción de query para MySQL"""
        self.remover.db_type = 'mysql'
        
        query = self.remover._build_delete_query('users', 'email, name', 'MAX')
        
        self.assertIn('DELETE FROM users', query)
        self.assertIn('MAX(id)', query) 
        self.assertIn('GROUP BY email, name', query)
        # MySQL SI necesita subconsulta extra
        self.assertIn('SELECT * FROM (', query)
        self.assertIn(') as temp', query)

class TestBackupManager(unittest.TestCase):
    """Tests para BackupManager"""
    
    def setUp(self):
        self.mock_connector = Mock()
        self.mock_engine = Mock()
        self.mock_connector.get_engine.return_value = self.mock_engine
        
        self.backup_manager = BackupManager(self.mock_connector)
    
    @patch('database_repair.backup_manager.datetime')
    def test_create_backup(self, mock_datetime):
        """Test creación de backup"""
        # Mock datetime
        mock_datetime.now.return_value.strftime.return_value = '20231201_120000'
        
        # Mock connection
        mock_conn = MagicMock()
        self.mock_engine.connect.return_value.__enter__.return_value = mock_conn
        
        backup_name = self.backup_manager.create_backup('users')
        
        expected_name = 'users_backup_20231201_120000'
        self.assertEqual(backup_name, expected_name)
        
        # Verificar que se ejecutó la query de backup
        mock_conn.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
    
    def test_verify_backup_success(self):
        """Test verificación exitosa de backup"""
        # Mock connection que retorna el mismo count para ambas tablas
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.side_effect = [[100], [100]]
        self.mock_engine.connect.return_value.__enter__.return_value = mock_conn
        
        result = self.backup_manager.verify_backup('users', 'users_backup')
        
        self.assertTrue(result)
    
    def test_verify_backup_failure(self):
        """Test verificación fallida de backup"""
        # Mock connection que retorna counts diferentes
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.side_effect = [[100], [95]]
        self.mock_engine.connect.return_value.__enter__.return_value = mock_conn
        
        result = self.backup_manager.verify_backup('users', 'users_backup')
        
        self.assertFalse(result)

class TestStatsCollector(unittest.TestCase):
    """Tests para StatsCollector"""
    
    def setUp(self):
        self.mock_connector = Mock()
        self.mock_engine = Mock()
        self.mock_connector.get_engine.return_value = self.mock_engine
        self.mock_connector.db_type = 'postgresql'
        
        self.stats_collector = StatsCollector(self.mock_connector)
    
    def test_get_table_stats_postgresql(self):
        """Test obtener estadísticas para PostgreSQL"""
        # Mock connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.side_effect = [
            [1000],  # COUNT(*)
            ['50 MB'],  # pg_size_pretty total
            ['5 MB']   # pg_size_pretty indexes
        ]
        self.mock_engine.connect.return_value.__enter__.return_value = mock_conn
        
        stats = self.stats_collector.get_table_stats('users')
        
        self.assertEqual(stats['total_records'], 1000)
        self.assertEqual(stats['table_size'], '50 MB')
        self.assertEqual(stats['index_size'], '5 MB')
    
    def test_compare_stats(self):
        """Test comparación de estadísticas"""
        before_stats = {'total_records': 1000, 'table_size': '50 MB'}
        after_stats = {'total_records': 800, 'table_size': '40 MB'}
        
        comparison = self.stats_collector.compare_stats(before_stats, after_stats)
        
        self.assertEqual(comparison['records_removed'], 200)
        self.assertEqual(comparison['reduction_percentage'], 20.0)
        self.assertEqual(comparison['before'], before_stats)
        self.assertEqual(comparison['after'], after_stats)

class TestIntegration(unittest.TestCase):
    """Tests de integración"""
    
    @patch('database_repair.database_connector.create_engine')
    def test_complete_workflow_dry_run(self, mock_create_engine):
        """Test workflow completo en modo dry run"""
        # Setup mocks
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        # Mock connection
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Mock resultados
        mock_conn.execute.return_value.fetchone.side_effect = [
            [5],  # count de registros a eliminar
        ]
        
        # Crear componentes
        connector = DatabaseConnector("sqlite:///test.db", "sqlite")
        remover = DuplicateRemover(connector)
        
        # Mock analyzer
        mock_df = pd.DataFrame({'email': ['test@test.com'], 'duplicate_count': [2]})
        remover.analyzer.analyze_duplicates = Mock(return_value=mock_df)
        
        # Ejecutar workflow
        result = remover.remove_duplicates_keep_oldest(
            'users', ['email'], dry_run=True
        )
        
        # Verificar resultados
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['deleted_count'], 5)
        self.assertTrue(result['dry_run'])
        self.assertIsNone(result['backup_table'])

def run_tests():
    """Ejecutar todos los tests"""
    # Configurar test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Agregar test classes
    test_classes = [
        TestDatabaseConnector,
        TestDuplicateAnalyzer, 
        TestDuplicateRemover,
        TestBackupManager,
        TestStatsCollector,
        TestIntegration
    ]
    
    for test_class in test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    
    # Ejecutar tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()

if __name__ == '__main__':
    print("🧪 EJECUTANDO TESTS UNITARIOS")
    print("=" * 50)
    
    success = run_tests()
    
    if success:
        print("\n✅ Todos los tests pasaron exitosamente")
        exit(0)
    else:
        print("\n❌ Algunos tests fallaron")
        exit(1)

