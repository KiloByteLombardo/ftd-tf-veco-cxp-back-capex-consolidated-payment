#!/usr/bin/env python3
"""
Consolidado CAPEX - Procesamiento de reportes financieros
Versión simplificada sin config.py
"""

import sys
import argparse
from pathlib import Path

# Importar módulos del proyecto
try:
    from countries.venezuela import procesar_venezuela, obtener_info_venezuela
    from utils import APIHelper
except ImportError as e:
    print(f"❌ Error importando módulos: {e}")
    print("💡 Asegúrate de que utils.py y venezuela.py estén en la misma carpeta")
    sys.exit(1)

# Configuración básica del proyecto
PAISES_DISPONIBLES = ['venezuela']
DEVELOPMENT_MODE = True  # Cambiar a False para producción

def main_cli(pais, archivo_reporte_pago, archivo_reporte_absoluto=None):
    """Procesamiento por línea de comandos"""
    try:
        print("🚀 CONSOLIDADO CAPEX")
        print("=" * 50)
        print(f"📊 País: {pais.upper()}")
        print(f"📄 Archivo: {archivo_reporte_pago}")
        if archivo_reporte_absoluto:
            print(f"📄 Archivo adicional: {archivo_reporte_absoluto}")
        print()

        # Verificar archivo
        if not Path(archivo_reporte_pago).exists():
            print(f"❌ Archivo no encontrado: {archivo_reporte_pago}")
            return False

        # Procesar según país
        resultado = None

        if pais.lower() == 'venezuela':
            resultado = procesar_venezuela(archivo_reporte_pago, archivo_reporte_absoluto)
        else:
            print(f"❌ País no soportado: {pais}")
            print(f"💡 Países disponibles: {PAISES_DISPONIBLES}")
            return False

        # Mostrar resultados
        if resultado:
            print()
            print("✅ PROCESO COMPLETADO")
            print("=" * 50)
            print(f"📁 Archivo: {resultado['archivo_salida']}")
            print(f"📊 Filas: {resultado['filas_procesadas']}")
            print(f"💱 Tasa: {resultado['tasa_utilizada']:.4f} {resultado['moneda']}/USD")
            print(f"🌍 País: {resultado['pais']}")
            return True
        else:
            print("❌ ERROR EN PROCESAMIENTO")
            return False

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main_gui():
    """Interfaz gráfica para desarrollo"""
    if not DEVELOPMENT_MODE:
        print("❌ GUI no disponible en modo producción")
        return

    try:
        print("🖥️ Iniciando interfaz gráfica...")
        from app import ConsolidadoCapexGUI

        gui = ConsolidadoCapexGUI()
        gui.run()

    except ImportError as e:
        print(f"❌ Error importando GUI: {e}")
        print("💡 GUI requiere tkinter y app.py")
    except Exception as e:
        print(f"❌ Error en GUI: {e}")

def mostrar_info():
    """Mostrar información del sistema"""
    print("⚙️ INFORMACIÓN DEL SISTEMA")
    print("=" * 50)
    print(f"Modo desarrollo: {'✅ SÍ' if DEVELOPMENT_MODE else '❌ NO'}")
    print(f"Países soportados: {', '.join(PAISES_DISPONIBLES)}")
    print()

    # Info específica de Venezuela
    if 'venezuela' in PAISES_DISPONIBLES:
        print("🇻🇪 VENEZUELA:")
        try:
            info = obtener_info_venezuela()
            for key, value in info.items():
                print(f"  {key}: {value}")
        except:
            print("  Error obteniendo información")
        print()

    # Probar conexión API
    print("🌐 PRUEBA DE CONECTIVIDAD:")
    try:
        api = APIHelper()
        tasa = api.obtener_tasa_venezuela()
        print(f"  BCV Venezuela: ✅ {tasa:.4f} VES/USD")
    except Exception as e:
        print(f"  BCV Venezuela: ❌ Error - {e}")

def mostrar_ayuda_uso():
    """Mostrar ejemplos de uso"""
    print()
    print("📋 EJEMPLOS DE USO:")
    print("=" * 30)
    print("# Interfaz gráfica (desarrollo)")
    print("python main.py")
    print()
    print("# Línea de comandos")
    print("python main.py --pais venezuela --archivo datos.xlsx")
    print()
    print("# Con archivo adicional")
    print("python main.py --pais venezuela --archivo datos.xlsx --adicional otros.xlsx")
    print()
    print("# Información del sistema")
    print("python main.py --info")

def main():
    """Función principal"""
    parser = argparse.ArgumentParser(
        description="Consolidado CAPEX - Procesamiento de reportes",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Argumentos
    parser.add_argument('--pais', 
                       choices=['venezuela'],
                       help='País a procesar')
    parser.add_argument('--archivo', 
                       help='Archivo de Reporte Pago Programado')
    parser.add_argument('--adicional', 
                       help='Archivo adicional (opcional)')
    parser.add_argument('--info', 
                       action='store_true',
                       help='Mostrar información del sistema')
    parser.add_argument('--cli', 
                       action='store_true',
                       help='Forzar modo línea de comandos')
    parser.add_argument('--version', 
                       action='version', 
                       version='Consolidado CAPEX v1.0')

    args = parser.parse_args()

    # Mostrar información
    if args.info:
        mostrar_info()
        mostrar_ayuda_uso()
        return

    # Modo línea de comandos
    if args.cli or (args.pais and args.archivo):
        if not args.pais or not args.archivo:
            print("❌ Modo CLI requiere --pais y --archivo")
            parser.print_help()
            mostrar_ayuda_uso()
            sys.exit(1)

        success = main_cli(args.pais, args.archivo, args.adicional)
        sys.exit(0 if success else 1)

    # Modo GUI (por defecto en desarrollo)
    elif DEVELOPMENT_MODE:
        main_gui()
    else:
        print("❌ Especifica argumentos para modo línea de comandos")
        parser.print_help()
        mostrar_ayuda_uso()

if __name__ == "__main__":
    main()
