"""
Script launcher principal - crear en la raíz del proyecto
"""
#!/usr/bin/env python3
import sys
import os

project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

def main():
    """Función principal del launcher"""
    print("🚀 SISTEMA DE REPARACIÓN DE DUPLICADOS")
    print("=" * 50)
    
    try:
        from database_repair.main import main as repair_main
        repair_main()
    except ImportError as e:
        print(f"❌ Error importando módulos: {str(e)}")
        print("\nVerifica que:")
        print("1. Todos los archivos estén en su lugar")
        print("2. Las dependencias estén instaladas: pip install -r requirements.txt")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error ejecutando aplicación: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
