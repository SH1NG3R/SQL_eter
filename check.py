
"""
Script para verificar que todo esté correctamente instalado
"""
#!/usr/bin/env python3
import sys
import os
import importlib.util

def check_python_version():
    """Verificar versión de Python"""
    print("🐍 Verificando versión de Python...")
    if sys.version_info < (3, 7):
        print("❌ Se requiere Python 3.7 o superior")
        return False
    else:
        print(f"✅ Python {sys.version.split()[0]} - OK")
        return True

def check_dependencies():
    """Verificar dependencias"""
    print("\n📦 Verificando dependencias...")
    
    required_packages = [
        'pandas',
        'sqlalchemy', 
        'psycopg2',
        'pymysql'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package} - Instalado")
        except ImportError:
            print(f"❌ {package} - No encontrado")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n⚠️  Paquetes faltantes: {', '.join(missing_packages)}")
        print("Ejecuta: pip install -r requirements.txt")
        return False
    
    return True

def check_project_structure():
    """Verificar estructura del proyecto"""
    print("\n📁 Verificando estructura del proyecto...")
    
    project_root = os.path.dirname(os.path.abspath(__file__))
    
    required_files = [
        'database_repair/__init__.py',
        'database_repair/config.py',
        'database_repair/database_connector.py',
        'database_repair/duplicate_analyzer.py',
        'database_repair/duplicate_remover.py',
        'database_repair/backup_manager.py',
        'database_repair/table_compressor.py',
        'database_repair/stats_collector.py',
        'database_repair/logger_setup.py',
        'database_repair/main.py',
        'support_utilities/cli.py',
        'support_utilities/example_usage.py',
        'support_utilities/setup.py',
        'support_utilities/test_duplicate_repair.py',
        'requirements.txt'
    ]
    
    missing_files = []
    
    for file_path in required_files:
        full_path = os.path.join(project_root, file_path)
        if os.path.exists(full_path):
            print(f"✅ {file_path}")
        else:
            print(f"❌ {file_path} - No encontrado")
            missing_files.append(file_path)
    
    if missing_files:
        print(f"\n⚠️  Archivos faltantes: {len(missing_files)}")
        return False
    
    return True

def check_imports():
    """Verificar que los imports funcionen"""
    print("\n🔗 Verificando imports...")
    
    # Agregar path del proyecto
    project_root = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, project_root)
    
    try:
        from database_repair import (
            DatabaseConnector, DuplicateAnalyzer, DuplicateRemover,
            BackupManager, TableCompressor, StatsCollector
        )
        print("✅ Imports del paquete principal - OK")
        
        # Test básico de instanciación (con mock)
        print("✅ Clases principales importadas correctamente")
        
    except ImportError as e:
        print(f"❌ Error en imports: {str(e)}")
        return False
    
    return True

def run_basic_tests():
    """Ejecutar tests básicos"""
    print("\n🧪 Ejecutando tests básicos...")
    
    try:
        # Cambiar al directorio de support_utilities para ejecutar tests
        original_dir = os.getcwd()
        support_dir = os.path.join(os.path.dirname(__file__), 'support_utilities')
        os.chdir(support_dir)
        
        # Importar y ejecutar algunos tests básicos
        sys.path.insert(0, os.path.dirname(support_dir))
        
        from test_duplicate_repair import TestDatabaseConnector
        import unittest
        
        # Ejecutar solo algunos tests básicos
        suite = unittest.TestSuite()
        suite.addTest(TestDatabaseConnector('test_invalid_db_type'))
        suite.addTest(TestDatabaseConnector('test_supported_db_types'))
        
        runner = unittest.TextTestRunner(verbosity=0, stream=open(os.devnull, 'w'))
        result = runner.run(suite)
        
        os.chdir(original_dir)
        
        if result.wasSuccessful():
            print("✅ Tests básicos - OK")
            return True
        else:
            print("❌ Algunos tests básicos fallaron")
            return False
            
    except Exception as e:
        print(f"❌ Error ejecutando tests: {str(e)}")
        return False

def main():
    """Función principal de verificación"""
    print("🔍 VERIFICACIÓN DE INSTALACIÓN")
    print("=" * 50)
    
    checks = [
        ("Versión de Python", check_python_version),
        ("Dependencias", check_dependencies),
        ("Estructura del proyecto", check_project_structure),
        ("Imports", check_imports),
        ("Tests básicos", run_basic_tests)
    ]
    
    all_passed = True
    
    for check_name, check_func in checks:
        try:
            if not check_func():
                all_passed = False
        except Exception as e:
            print(f"❌ Error en {check_name}: {str(e)}")
            all_passed = False
    
    print("\n" + "=" * 50)
    
    if all_passed:
        print("🎉 ¡INSTALACIÓN VERIFICADA EXITOSAMENTE!")
        print("\nPuedes usar el sistema de las siguientes formas:")
        print("1. Script principal: python run.py")
        print("2. CLI: python run_cli.py --help")
        print("3. Tests completos: cd support_utilities && python test_duplicate_repair.py")
        print("4. Ejemplos: cd support_utilities && python example_usage.py")
    else:
        print("⚠️  HAY PROBLEMAS EN LA INSTALACIÓN")
        print("\nRevisa los errores arriba y:")
        print("1. Instala dependencias: pip install -r requirements.txt")
        print("2. Verifica que todos los archivos estén presentes")
        print("3. Ejecuta este script nuevamente")
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)