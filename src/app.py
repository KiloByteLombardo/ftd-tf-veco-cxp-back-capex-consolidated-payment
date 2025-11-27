import sys
from pathlib import Path
import threading

# Tkinter
try:
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
except ImportError:
    print("❌ tkinter no disponible")
    sys.exit(1)

# Módulos del proyecto
try:
    from countries.venezuela import procesar_venezuela
    from utils import APIHelper
except ImportError as e:
    print(f"❌ Error importando módulos: {e}")
    sys.exit(1)

class ConsolidadoCapexGUI:
    """Interfaz gráfica simple para Consolidado CAPEX"""

    def __init__(self):
        self.root = tk.Tk()
        self.procesando = False
        self.setup_ui()

    def setup_ui(self):
        """Configurar interfaz"""
        self.root.title("🏢 Consolidado CAPEX")
        self.root.geometry("600x500")

        # Frame principal
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Título
        ttk.Label(main_frame, text="🏢 Consolidado CAPEX", 
                 font=('Arial', 16, 'bold')).grid(row=0, column=0, columnspan=2, pady=10)

        # País
        ttk.Label(main_frame, text="País:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.pais_var = tk.StringVar(value="Venezuela")
        ttk.Combobox(main_frame, textvariable=self.pais_var, 
                    values=["Venezuela"], state='readonly', width=30).grid(row=1, column=1, pady=5)

        # Archivo principal
        ttk.Label(main_frame, text="Reporte Pago Programado:").grid(row=2, column=0, sticky=tk.W, pady=5)

        archivo_frame = ttk.Frame(main_frame)
        archivo_frame.grid(row=2, column=1, sticky=(tk.W, tk.E), pady=5)

        self.archivo_var = tk.StringVar()
        ttk.Entry(archivo_frame, textvariable=self.archivo_var, width=40).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(archivo_frame, text="📁", width=3, 
                  command=self.seleccionar_archivo).pack(side=tk.RIGHT)

        # Archivo adicional
        ttk.Label(main_frame, text="Archivo Adicional (opcional):").grid(row=3, column=0, sticky=tk.W, pady=5)

        adicional_frame = ttk.Frame(main_frame)
        adicional_frame.grid(row=3, column=1, sticky=(tk.W, tk.E), pady=5)

        self.adicional_var = tk.StringVar()
        ttk.Entry(adicional_frame, textvariable=self.adicional_var, width=40).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(adicional_frame, text="📁", width=3, 
                  command=self.seleccionar_adicional).pack(side=tk.RIGHT)

        # Botones
        botones_frame = ttk.Frame(main_frame)
        botones_frame.grid(row=4, column=0, columnspan=2, pady=20)

        self.procesar_btn = ttk.Button(botones_frame, text="🚀 Procesar", 
                                      command=self.procesar)
        self.procesar_btn.pack(side=tk.LEFT, padx=5)

        ttk.Button(botones_frame, text="🧹 Limpiar", 
                  command=self.limpiar).pack(side=tk.LEFT, padx=5)

        ttk.Button(botones_frame, text="ℹ️ Info", 
                  command=self.mostrar_info).pack(side=tk.LEFT, padx=5)

        # Progreso
        self.progress = ttk.Progressbar(main_frame, mode='indeterminate')
        self.progress.grid(row=5, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=10)

        # Área de resultados
        ttk.Label(main_frame, text="Resultados:").grid(row=6, column=0, sticky=tk.W)

        text_frame = ttk.Frame(main_frame)
        text_frame.grid(row=7, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)

        self.resultado_text = tk.Text(text_frame, height=12, font=('Consolas', 9))
        scrollbar = ttk.Scrollbar(text_frame, orient=tk.VERTICAL, command=self.resultado_text.yview)
        self.resultado_text.configure(yscrollcommand=scrollbar.set)

        self.resultado_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Configurar redimensionamiento
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(7, weight=1)

        # Log inicial
        self.log("🖥️ Interfaz iniciada")
        self.log("💡 Selecciona archivo y presiona Procesar")
        self.log("")

    def seleccionar_archivo(self):
        """Seleccionar archivo principal"""
        archivo = filedialog.askopenfilename(
            title="Seleccionar Reporte Pago Programado",
            filetypes=[("Excel", "*.xlsx *.xls"), ("Todos", "*.*")]
        )
        if archivo:
            self.archivo_var.set(archivo)
            self.log(f"📂 Archivo: {Path(archivo).name}")

    def seleccionar_adicional(self):
        """Seleccionar archivo adicional"""
        archivo = filedialog.askopenfilename(
            title="Seleccionar Archivo Adicional",
            filetypes=[("Excel", "*.xlsx *.xls"), ("Todos", "*.*")]
        )
        if archivo:
            self.adicional_var.set(archivo)
            self.log(f"📂 Adicional: {Path(archivo).name}")

    def limpiar(self):
        """Limpiar campos"""
        self.archivo_var.set("")
        self.adicional_var.set("")
        self.resultado_text.delete(1.0, tk.END)
        self.log("🧹 Campos limpiados")

    def mostrar_info(self):
        """Mostrar información del sistema"""
        info_window = tk.Toplevel(self.root)
        info_window.title("ℹ️ Información")
        info_window.geometry("400x300")
        info_window.transient(self.root)

        info_text = tk.Text(info_window, wrap=tk.WORD, padx=10, pady=10)
        info_text.pack(fill=tk.BOTH, expand=True)

        # Obtener información
        try:
            api = APIHelper()
            tasa = api.obtener_tasa_venezuela()

            info_content = f"""⚙️ CONSOLIDADO CAPEX
{"=" * 30}

🌍 Países soportados:
  • Venezuela

🇻🇪 Venezuela:
  • Moneda: VES
  • API: DolarApi.com (BCV)
  • Tasa actual: {tasa:.4f} VES/USD
  • Archivo salida: ConsolidadoCapexVENEZUELA.xlsx

📊 Funcionalidad:
  • Hoja destino: BOSQUETO
  • Fórmula: =SI(L3="VES";K3/Tasa;K3)
  • Color: Verde
  • Conversión: VEF → VES automática

🔗 Versión: v1.0
"""
        except Exception as e:
            info_content = f"Error obteniendo información: {e}"

        info_text.insert(tk.END, info_content)
        info_text.config(state=tk.DISABLED)

        ttk.Button(info_window, text="Cerrar", 
                  command=info_window.destroy).pack(pady=10)

    def log(self, mensaje):
        """Agregar mensaje al log"""
        self.resultado_text.insert(tk.END, f"{mensaje}\n")
        self.resultado_text.see(tk.END)
        self.root.update_idletasks()

    def validar_entrada(self):
        """Validar entrada"""
        if not self.archivo_var.get().strip():
            messagebox.showerror("Error", "Selecciona el archivo principal")
            return False

        if not Path(self.archivo_var.get()).exists():
            messagebox.showerror("Error", "El archivo no existe")
            return False

        return True

    def procesar(self):
        """Procesar archivo"""
        if self.procesando:
            return

        if not self.validar_entrada():
            return

        self.procesando = True
        self.procesar_btn.config(state='disabled', text='⏳ Procesando...')
        self.progress.start()

        thread = threading.Thread(target=self._procesar_thread, daemon=True)
        thread.start()

    def _procesar_thread(self):
        """Hilo de procesamiento"""
        try:
            archivo = self.archivo_var.get()
            adicional = self.adicional_var.get() or None

            self.log("🚀 INICIANDO PROCESAMIENTO")
            self.log("=" * 40)
            self.log(f"📄 Archivo: {Path(archivo).name}")
            if adicional:
                self.log(f"📄 Adicional: {Path(adicional).name}")

            # Procesar Venezuela
            resultado = procesar_venezuela(archivo, adicional)

            if resultado:
                self.log("✅ COMPLETADO EXITOSAMENTE")
                self.log("=" * 40)
                self.log(f"📁 Archivo: {resultado['archivo_salida']}")
                self.log(f"📊 Filas: {resultado['filas_procesadas']}")
                self.log(f"💱 Tasa: {resultado['tasa_utilizada']:.4f} VES/USD")

                # Notificación
                self.root.after(0, lambda: messagebox.showinfo("✅ Éxito", 
                    f"Archivo generado:\n{resultado['archivo_salida']}\n\n"
                    f"Filas: {resultado['filas_procesadas']}"))
            else:
                self.log("❌ ERROR EN PROCESAMIENTO")
                error_msg = str(e) 
                self.root.after(0, lambda: messagebox.showerror("❌ Error", 
                    error_msg))

        except Exception as e:
            self.log(f"❌ EXCEPCIÓN: {e}")
            error_msg = str(e)
            self.root.after(0, lambda: messagebox.showerror("❌ Error", error_msg))

        finally:
            self.procesando = False
            self.root.after(0, self._restaurar_interfaz)

    def _restaurar_interfaz(self):
        """Restaurar interfaz"""
        self.procesar_btn.config(state='normal', text='🚀 Procesar')
        self.progress.stop()

    def run(self):
        """Ejecutar aplicación"""
        # Centrar ventana
        self.root.update_idletasks()
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        x = (self.root.winfo_screenwidth() // 2) - (width // 2)
        y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.root.geometry(f'{width}x{height}+{x}+{y}')

        self.root.mainloop()

if __name__ == "__main__":
    app = ConsolidadoCapexGUI()
    app.run()
