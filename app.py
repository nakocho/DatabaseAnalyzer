import os
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
from flask import Flask, render_template, request, redirect, url_for, flash, send_file
from werkzeug.utils import secure_filename
from werkzeug.middleware.proxy_fix import ProxyFix
import logging
from validators import validar_identificador, limpiar_y_elegir_telefono, validar_email_Regex
import uuid
from datetime import datetime
import pymysql
from pymysql.cursors import DictCursor

# Configure logging
logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "default_secret_key_for_dev")
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# Configuration
UPLOAD_FOLDER = 'static/uploads'
DOWNLOAD_FOLDER = 'static/downloads'
ALLOWED_EXTENSIONS = {'csv'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['DOWNLOAD_FOLDER'] = DOWNLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Ensure directories exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

def get_database_connection():
    """Get database connection using environment variables"""
    try:
        connection = pymysql.connect(
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT', 3306)),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME'),
            cursorclass=DictCursor,
            charset='utf8mb4'
        )
        return connection
    except Exception as e:
        logging.error(f"Error connecting to database: {str(e)}")
        return None

def test_database_connection():
    """Test database connection and return status"""
    try:
        connection = get_database_connection()
        if connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT VERSION() as version")
                result = cursor.fetchone()
                connection.close()
                return True, f"Connected to MariaDB {result['version']}"
        else:
            return False, "Unable to establish connection"
    except Exception as e:
        return False, f"Database error: {str(e)}"

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def puede_escribir_archivo(ruta):
    """Check if file can be written"""
    try:
        with open(ruta, 'a'):
            return True
    except PermissionError:
        return False

@app.route('/')
def index():
    """Main page with file upload form"""
    return render_template('index.html')

@app.route('/test-db')
def test_db():
    """Test database connection"""
    success, message = test_database_connection()
    if success:
        flash(f'✅ Conexión exitosa: {message}', 'success')
    else:
        flash(f'❌ Error de conexión: {message}', 'error')
    return redirect(url_for('index'))

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload and processing"""
    if 'file' not in request.files:
        flash('No se seleccionó ningún archivo', 'error')
        return redirect(request.url)
    
    file = request.files['file']
    if file.filename == '':
        flash('No se seleccionó ningún archivo', 'error')
        return redirect(request.url)
    
    if file and allowed_file(file.filename):
        try:
            # Generate unique filename
            file_id = str(uuid.uuid4())
            filename = secure_filename(file.filename)
            upload_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{file_id}_{filename}")
            file.save(upload_path)
            
            # Process the file
            results = process_csv_file(upload_path, file_id)
            
            # Clean up uploaded file
            os.remove(upload_path)
            
            return render_template('results.html', results=results, file_id=file_id)
            
        except Exception as e:
            logging.error(f"Error processing file: {str(e)}")
            flash(f'Error al procesar el archivo: {str(e)}', 'error')
            return redirect(url_for('index'))
    else:
        flash('Tipo de archivo no permitido. Solo se aceptan archivos CSV.', 'error')
        return redirect(url_for('index'))

def process_csv_file(file_path, file_id):
    """Process CSV file and validate data"""
    try:
        logging.info(f"Starting to process CSV file: {file_path}")

        # Read CSV file
        df = pd.read_csv(file_path, sep=';', encoding='utf-8-sig')
        logging.info(f"CSV loaded successfully. Total records: {len(df)}")
        logging.info(f"Columns found: {list(df.columns)}")
        

        # Lists for valid and invalid records
        validos = []
        no_validos = []
        warnings = []

        # Process each row
        total_rows = len(df)
        for idx, fila in df.iterrows():
            if idx % 100 == 0:
                logging.info(f"Processing row {idx + 1}/{total_rows}")

            # add new column old_user
            fila['old_user'] = 1

            # Clean phone number
            fila['telefono'] = limpiar_y_elegir_telefono(fila.get('telefono', ''))
            
            # Validate DNI/NIE/CIF
            dni = str(fila.get('dni', '')).strip()
            dni_valido, motivo_dni = validar_identificador(dni)
            
            # Validate email if present using new regex function
            email_valido = True
            motivo_email = ""
            email_normalizado = ""
            email_original = ""

            from validators import validar_email_Regex
            email_valido, motivo_email, email_normalizado, email_original = validar_email_Regex(fila.get('email', ''))
            # Always update the email field with normalized/corrected version
            fila['email'] = email_normalizado

            # Determine record status
            if dni_valido:
                # DNI is valid, add to validos
                validos.append(fila.copy())
                
                # If email is invalid, also add to warnings
                if not email_valido :
                    fila_warning = fila.copy()
                    fila_warning['email_original'] = email_original
                    fila_warning['motivo_warning'] = f"Email: {motivo_email}"
                    warnings.append(fila_warning)
            else:
                # DNI is invalid, add to no_validos
                fila_con_motivo = fila.copy()
                motivos = [f"DNI: {motivo_dni}"]
                
                if not email_valido:
                    motivos.append(f"Email: {motivo_email}")
                
                fila_con_motivo['motivo_invalido'] = "; ".join(motivos)
                no_validos.append(fila_con_motivo)
        
        # Create DataFrames
        df_validos = pd.DataFrame(validos)
        df_no_validos = pd.DataFrame(no_validos)
        df_warnings = pd.DataFrame(warnings)
        
        # Generate output files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        valid_filename = f"usuarios_validos_{timestamp}.csv"
        invalid_filename = f"usuarios_invalidos_{timestamp}.csv"
        warning_filename = f"usuarios_advertencias_{timestamp}.csv"

        valid_path = os.path.join(app.config['DOWNLOAD_FOLDER'], f"{file_id}_{valid_filename}")
        invalid_path = os.path.join(app.config['DOWNLOAD_FOLDER'], f"{file_id}_{invalid_filename}")
        warning_path = os.path.join(app.config['DOWNLOAD_FOLDER'], f"{file_id}_{warning_filename}")
        
        # Save files
        if len(df_validos) > 0:
            df_validos.to_csv(valid_path, sep=';', index=False, encoding='utf-8-sig')
        
        if len(df_no_validos) > 0:
            df_no_validos.to_csv(invalid_path, sep=';', index=False, encoding='utf-8-sig')

        if len(df_warnings) > 0:
            df_warnings.to_csv(warning_path, sep=';', index=False, encoding='utf-8-sig')

        # Prepare results
        results = {
            'total_records': len(df),
            'valid_records': len(df_validos),
            'invalid_records': len(df_no_validos),
            'warning_records': len(df_warnings),
            'valid_file': f"{file_id}_{valid_filename}" if len(df_validos) > 0 else None,
            'invalid_file': f"{file_id}_{invalid_filename}" if len(df_no_validos) > 0 else None,
            'warning_file': f"{file_id}_{warning_filename}" if len(df_warnings) > 0 else None,
            'columns': list(df.columns),
            'invalid_reasons': df_no_validos['motivo_invalido'].value_counts().to_dict() if len(df_no_validos) > 0 else {},
            'warning_reasons': df_warnings['motivo_warning'].value_counts().to_dict() if len(df_warnings) > 0 else {}
        }
        
        return results
        
    except Exception as e:
        logging.error(f"Error in process_csv_file: {str(e)}")
        raise e

@app.route('/download/<filename>')
def download_file(filename):
    """Download processed file"""
    try:
        file_path = os.path.join(app.config['DOWNLOAD_FOLDER'], filename)
        if os.path.exists(file_path):
            return send_file(file_path, as_attachment=True)
        else:
            flash('Archivo no encontrado', 'error')
            return redirect(url_for('index'))
    except Exception as e:
        logging.error(f"Error downloading file: {str(e)}")
        flash('Error al descargar el archivo', 'error')
        return redirect(url_for('index'))

@app.errorhandler(413)
def too_large(e):
    """Handle file too large error"""
    flash('El archivo es demasiado grande. Máximo 16MB permitido.', 'error')
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
