from flask import Flask, render_template, request, redirect, url_for, flash
import os
import mysql.connector
from mysql.connector import Error
import pandas as pd
from werkzeug.utils import secure_filename
import openpyxl  # For Excel support

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max upload

# Ensure upload directory exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls', 'json', 'txt'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def create_mysql_connection(host, user, password, database=None):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        return connection
    except Error as e:
        flash(f"Error connecting to MySQL: {e}", 'error')
        return None

def read_data_file(file_path, file_extension):
    try:
        if file_extension == 'csv':
            return pd.read_csv(file_path)
        elif file_extension in ('xlsx', 'xls'):
            return pd.read_excel(file_path)
        elif file_extension == 'json':
            return pd.read_json(file_path)
        elif file_extension == 'txt':
            # Try reading as space or tab separated
            try:
                return pd.read_csv(file_path, sep='\t')
            except:
                return pd.read_csv(file_path, sep='\s+')
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")
    except Exception as e:
        raise ValueError(f"Error reading file: {e}")

def create_table_from_data(connection, table_name, file_path, file_extension):
    try:
        # Read data file based on extension
        df = read_data_file(file_path, file_extension)
        
        # Generate CREATE TABLE SQL
        create_table_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ("    
        
        # Map pandas dtypes to MySQL types
        type_mapping = {
            'int64': 'INT',
            'float64': 'FLOAT',
            'object': 'VARCHAR(255)',
            'bool': 'BOOLEAN',
            'datetime64': 'DATETIME'
        }
        
        columns = []
        for col_name, dtype in zip(df.columns, df.dtypes):
            mysql_type = type_mapping.get(str(dtype), 'VARCHAR(255)')
            columns.append(f"`{col_name}` {mysql_type}")
        
        create_table_sql += ", ".join(columns) + ")"
        
        # Execute CREATE TABLE
        cursor = connection.cursor()
        cursor.execute(create_table_sql)
        
        # Insert data
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_sql = f"INSERT INTO `{table_name}` ({', '.join([f'`{col}`' for col in df.columns])}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples, handling NaN values
        data = []
        for row in df.itertuples(index=False):
            row_data = []
            for value in row:
                if pd.isna(value):
                    row_data.append(None)
                else:
                    row_data.append(value)
            data.append(tuple(row_data))
        
        cursor.executemany(insert_sql, data)
        connection.commit()
        
        return True, len(df)
    except Error as e:
        connection.rollback()
        return False, f"Error creating table or inserting data: {e}"
    except Exception as e:
        return False, f"Error processing file: {e}"
    finally:
        if 'cursor' in locals():
            cursor.close()

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Get form data
        host = request.form.get('host', 'localhost')
        user = request.form.get('user')
        password = request.form.get('password')
        database = request.form.get('database')
        table_name = request.form.get('table_name')
        
        # Check if file was uploaded
        if 'file' not in request.files:
            flash('No file uploaded', 'error')
            return redirect(request.url)
        
        file = request.files['file']
        if file.filename == '':
            flash('No selected file', 'error')
            return redirect(request.url)
        
        if not (host and user and database and table_name):
            flash('Please fill all required fields', 'error')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_extension = filename.rsplit('.', 1)[1].lower()
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            
            try:
                # Connect to MySQL (without specific database first to check credentials)
                connection = create_mysql_connection(host, user, password)
                if not connection:
                    return redirect(request.url)
                
                # Create database if not exists
                try:
                    cursor = connection.cursor()
                    cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database}`")
                    cursor.close()
                    connection.close()
                    
                    # Reconnect with the specific database
                    connection = create_mysql_connection(host, user, password, database)
                    if not connection:
                        return redirect(request.url)
                    
                    # Create table and insert data
                    success, result = create_table_from_data(connection, table_name, file_path, file_extension)
                    
                    if success:
                        flash(f"Success! Created table '{table_name}' with {result} records.", 'success')
                        return redirect(url_for('success', table_name=table_name, record_count=result))
                    else:
                        flash(result, 'error')
                except Error as e:
                    flash(f"MySQL Error: {e}", 'error')
                finally:
                    if connection and connection.is_connected():
                        connection.close()
                
            finally:
                # Clean up uploaded file
                if os.path.exists(file_path):
                    os.remove(file_path)
        else:
            flash('File type not allowed. Supported formats: CSV, Excel (XLSX, XLS), JSON, TXT', 'error')
    
    return render_template('index.html')

@app.route('/success')
def success():
    table_name = request.args.get('table_name')
    record_count = request.args.get('record_count')
    return render_template('success.html', table_name=table_name, record_count=record_count)

if __name__ == '__main__':
    app.run(debug=True)