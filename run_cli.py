"""
Script CLI launcher
"""
#!/usr/bin/env python3
import sys
import os

# Agregar el directorio del proyecto al path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

def main():
    """Función principal del CLI launcher"""
    try:
        from support_utilities.cli import main_cli
        main_cli()
    except ImportError as e:
        print(f"❌ Error importando CLI: {str(e)}")
        print("\nVerifica que el archivo support_utilities/cli.py existe")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error ejecutando CLI: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
