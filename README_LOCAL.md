# Instalación y Uso Local - Validador de Datos Españoles

## Requisitos
- Python 3.8 o superior
- pip (administrador de paquetes de Python)

## Instalación

### 1. Descargar archivos
Descarga todos los archivos del proyecto a una carpeta local:
- `app.py`
- `main.py` 
- `validators.py`
- `.env`
- `dependencias.txt`
- Carpeta `templates/` (con `index.html` y `results.html`)
- Carpeta `static/` (con subcarpetas `uploads/` y `downloads/`)

### 2. Crear entorno virtual (recomendado)
```bash
python -m venv venv

# En Windows:
venv\Scripts\activate

# En Linux/Mac:
source venv/bin/activate
```

### 3. Instalar dependencias
```bash
pip install -r dependencias.txt
```

### 4. Configurar variables de entorno
Edita el archivo `.env` con tus datos reales:
```
# Database Configuration
DB_HOST=172.16.20.121
DB_PORT=3306
DB_USER=admin-arabat
DB_PASSWORD=777_nbczST
DB_NAME=arabatDB

# Flask Configuration
SESSION_SECRET=cambia_esto_por_una_clave_segura_aleatoria
FLASK_ENV=development
FLASK_DEBUG=True
```

## Ejecución

### Opción 1: Usando Python directamente
```bash
python app.py
```

### Opción 2: Usando el archivo main
```bash
python main.py
```

### Opción 3: Usando Gunicorn (producción)
```bash
gunicorn --bind 0.0.0.0:5000 --reload main:app
```

## Acceso
Abre tu navegador y ve a: `http://localhost:5000`

## Uso

1. **Subir archivo CSV**: Debe tener las columnas `dni`, `telefono`, `email` (separadas por punto y coma `;`)
2. **Probar base de datos**: Haz clic en "Probar Conexión Base de Datos"
3. **Procesar datos**: Sube tu archivo y recibe los resultados con registros válidos e inválidos

## Estructura de archivos CSV

### Formato esperado:
```csv
dni;telefono;email;nombre;apellidos
12345678Z;666123456;usuario@email.com;Juan;Pérez
X1234567L;91-123-45-67;otro@correo.es;María;García
```

### Validaciones que se aplican:
- **DNI**: 8 dígitos + letra de control
- **NIE**: X/Y/Z + 7 dígitos + letra de control  
- **CIF**: Letra + 7 dígitos + dígito/letra de control
- **Email**: Validación RFC compliant + normalización
- **Teléfono**: Limpieza automática, prioriza móviles (6/7) sobre fijos (8/9)

## Troubleshooting

### Error de conexión a base de datos
Si no puedes conectar a la base de datos:
1. Verifica que la IP `172.16.20.121` sea accesible desde tu red
2. Confirma que el puerto 3306 esté abierto
3. La aplicación funciona completamente sin base de datos para procesar CSV

### Error de módulos no encontrados
```bash
pip install --upgrade pip
pip install -r dependencias.txt
```

### Puerto ocupado
Si el puerto 5000 está ocupado, cambia en `app.py`:
```python
app.run(host='0.0.0.0', port=8000, debug=True)  # Cambiar puerto
```