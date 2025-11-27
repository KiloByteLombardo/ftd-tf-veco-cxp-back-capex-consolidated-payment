FROM python:3.11-slim

WORKDIR /app

# Copiar requirements
COPY requirements.txt .

# Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código de la aplicación
COPY . .

# Exponer puerto (valor por defecto si no se especifica)
EXPOSE 5000

# Comando para ejecutar
CMD ["python", "src/api.py"]
