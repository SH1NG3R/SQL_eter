"""
Configuración de instalación del paquete
"""
import sys
import os


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from setuptools import setup, find_packages

def read_requirements():
    requirements_path = os.path.join(os.path.dirname(__file__), '..', 'requirements.txt')
    try:
        with open(requirements_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    except FileNotFoundError:
        return [
            "pandas>=1.3.0",
            "sqlalchemy>=1.4.0", 
            "psycopg2-binary>=2.9.0",
            "pymysql>=1.0.0",
        ]

def read_long_description():
    readme_path = os.path.join(os.path.dirname(__file__), '..', 'README.md')
    try:
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        return "Sistema modular para reparación de duplicados en bases de datos"

setup(
    name="database-duplicate-repair",
    version="1.0.0",
    description="Sistema modular para reparación de duplicados en bases de datos",
    long_description=read_long_description(),
    long_description_content_type="text/markdown",
    author="Tu Nombre",
    author_email="tu.email@ejemplo.com",
    url="https://github.com/tu-usuario/database-duplicate-repair",
    
    packages=find_packages(where='..'),
    package_dir={'': '..'},
    install_requires=read_requirements(),
    python_requires=">=3.11",
    
    entry_points={
        'console_scripts': [
            'repair-duplicates=database_repair.main:main',
            'repair-duplicates-cli=support_utilities.cli:main_cli',
        ],
    },
    
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.11",
    ],
    
    keywords="database, duplicates, repair, cleanup, postgresql, mysql, sqlite",
    
    include_package_data=True,
    package_data={
        'database_repair': ['*.py'],
        'support_utilities': ['*.py'],
    },
    
    extras_require={
        'dev': [
            'pytest>=6.0',
            'pytest-cov>=2.0',
            'black>=21.0',
            'flake8>=3.8',
        ],
        'docs': [
            'sphinx>=4.0',
            'sphinx-rtd-theme>=0.5',
        ],
    },
)