# =================== ARCHIVO: tasas.py ===================
"""
Módulo para consulta de tasas de cambio desde BigQuery.
Maneja las tasas BCV desde la tabla bcv_tasas.
"""

import os
from google.cloud import bigquery
from google.oauth2 import service_account
from typing import Optional, Dict
import datetime

# Configuración
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BIGQUERY_DATASET_TASAS = 'cxp_vzla'  # Dataset donde está la tabla de tasas
BIGQUERY_TABLE_TASAS = 'bcv_tasas'   # Tabla de tasas BCV
CREDENTIALS_FILE = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')


class TasasBCVHelper:
    """
    Helper para consultar tasas BCV desde BigQuery.
    Tabla: cxp_vzla.bcv_tasas
    Campos: Date (DATE), USD (FLOAT), EUR (FLOAT)
    """
    
    def __init__(self):
        self.client = None
        self.tasas_cache: Dict[str, float] = {}  # Cache: fecha -> tasa USD
        self._cache_cargado = False
    
    def _crear_cliente(self) -> bigquery.Client:
        """Crear cliente de BigQuery usando credenciales disponibles."""
        if self.client is not None:
            return self.client
        
        try:
            if CREDENTIALS_FILE and os.path.exists(CREDENTIALS_FILE):
                credentials = service_account.Credentials.from_service_account_file(
                    CREDENTIALS_FILE,
                    scopes=["https://www.googleapis.com/auth/bigquery"]
                )
                self.client = bigquery.Client(
                    credentials=credentials,
                    project=GCP_PROJECT_ID
                )
                print(f"✅ TasasBCVHelper: Cliente BigQuery creado (credenciales archivo)")
            else:
                # Usar Application Default Credentials (ADC)
                self.client = bigquery.Client(project=GCP_PROJECT_ID)
                print(f"✅ TasasBCVHelper: Cliente BigQuery creado (ADC)")
            
            return self.client
        except Exception as e:
            print(f"❌ TasasBCVHelper: Error creando cliente BigQuery: {e}")
            raise
    
    def cargar_todas_las_tasas(self) -> Dict[str, float]:
        """
        Cargar todas las tasas BCV desde BigQuery en cache.
        Se ejecuta una sola vez para evitar múltiples consultas.
        
        Returns:
            Dict[str, float]: Diccionario fecha (YYYY-MM-DD) -> tasa USD
        """
        if self._cache_cargado:
            return self.tasas_cache
        
        try:
            client = self._crear_cliente()
            
            table_id = f"`{GCP_PROJECT_ID}.{BIGQUERY_DATASET_TASAS}.{BIGQUERY_TABLE_TASAS}`"
            
            query = f"""
            SELECT 
                FORMAT_DATE('%Y-%m-%d', Date) as fecha,
                USD as tasa_usd
            FROM {table_id}
            WHERE USD IS NOT NULL
            ORDER BY Date DESC
            """
            
            print(f"💱 TasasBCVHelper: Cargando tasas desde {table_id}...")
            
            query_job = client.query(query)
            results = query_job.result(timeout=60)
            
            for row in results:
                self.tasas_cache[row.fecha] = float(row.tasa_usd)
            
            self._cache_cargado = True
            print(f"✅ TasasBCVHelper: {len(self.tasas_cache)} tasas cargadas en cache")
            
            # Mostrar últimas 3 fechas como ejemplo
            if self.tasas_cache:
                fechas_recientes = sorted(self.tasas_cache.keys(), reverse=True)[:3]
                for fecha in fechas_recientes:
                    print(f"   📌 {fecha}: {self.tasas_cache[fecha]:.4f} VES/USD")
            
            return self.tasas_cache
            
        except Exception as e:
            print(f"❌ TasasBCVHelper: Error cargando tasas: {e}")
            return {}
    
    def obtener_tasa_bcv_para_fecha(self, fecha) -> float:
        """
        Obtener la tasa BCV (USD) para una fecha específica.
        
        Args:
            fecha: Fecha en formato string 'YYYY-MM-DD', datetime.date, o datetime.datetime
        
        Returns:
            float: Tasa USD o 0 si no se encuentra
        """
        # Normalizar fecha a string YYYY-MM-DD
        if isinstance(fecha, datetime.datetime):
            fecha_str = fecha.strftime('%Y-%m-%d')
        elif isinstance(fecha, datetime.date):
            fecha_str = fecha.strftime('%Y-%m-%d')
        elif isinstance(fecha, str):
            # Limpiar formato
            fecha_str = fecha.split('T')[0].split(' ')[0]  # Quitar tiempo si existe
            # Validar formato básico
            if len(fecha_str) == 10 and fecha_str[4] == '-' and fecha_str[7] == '-':
                pass  # Ya está en formato correcto
            else:
                # Intentar parsear otros formatos comunes
                try:
                    for fmt in ['%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y']:
                        try:
                            dt = datetime.datetime.strptime(fecha_str, fmt)
                            fecha_str = dt.strftime('%Y-%m-%d')
                            break
                        except ValueError:
                            continue
                except:
                    pass
        else:
            return 0.0
        
        # Cargar cache si no está cargado
        if not self._cache_cargado:
            self.cargar_todas_las_tasas()
        
        # Buscar en cache
        tasa = self.tasas_cache.get(fecha_str, 0.0)
        
        return tasa
    
    def obtener_tasa_bcv_mas_reciente(self) -> tuple:
        """
        Obtener la tasa BCV más reciente disponible.
        
        Returns:
            tuple: (tasa, fecha_str) o (0, None) si no hay datos
        """
        if not self._cache_cargado:
            self.cargar_todas_las_tasas()
        
        if not self.tasas_cache:
            return 0.0, None
        
        # Obtener la fecha más reciente
        fecha_reciente = max(self.tasas_cache.keys())
        tasa = self.tasas_cache[fecha_reciente]
        
        return tasa, fecha_reciente
    
    def limpiar_cache(self):
        """Limpiar el cache de tasas para forzar recarga."""
        self.tasas_cache.clear()
        self._cache_cargado = False
        print("🔄 TasasBCVHelper: Cache limpiado")


# Instancia global para reutilización
_tasas_helper: Optional[TasasBCVHelper] = None


def obtener_helper_tasas() -> TasasBCVHelper:
    """
    Obtener instancia singleton del helper de tasas.
    
    Returns:
        TasasBCVHelper: Instancia del helper
    """
    global _tasas_helper
    if _tasas_helper is None:
        _tasas_helper = TasasBCVHelper()
    return _tasas_helper


def obtener_tasa_bcv(fecha) -> float:
    """
    Función de conveniencia para obtener tasa BCV para una fecha.
    
    Args:
        fecha: Fecha en cualquier formato soportado
    
    Returns:
        float: Tasa USD o 0 si no se encuentra
    """
    helper = obtener_helper_tasas()
    return helper.obtener_tasa_bcv_para_fecha(fecha)


def precargar_tasas_bcv() -> Dict[str, float]:
    """
    Pre-cargar todas las tasas BCV en cache.
    Útil para llamar al inicio del procesamiento.
    
    Returns:
        Dict[str, float]: Diccionario de tasas
    """
    helper = obtener_helper_tasas()
    return helper.cargar_todas_las_tasas()
