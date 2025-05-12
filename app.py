from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import os
import mysql.connector
from mysql.connector import Error
import pandas as pd
from werkzeug.utils import secure_filename
import numpy as np

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

def detect_mysql_type(series):
    """
    Detect the most appropriate MySQL data type for a pandas Series
    Returns (type_name, max_length) tuple
    """
    dtype = str(series.dtype)
    
    # For numeric columns
    if dtype.startswith('int'):
        max_val = series.max()
        min_val = series.min()
        
        if -128 <= min_val <= max_val <= 127:
            return 'TINYINT', None
        elif -32768 <= min_val <= max_val <= 32767:
            return 'SMALLINT', None
        elif -2147483648 <= min_val <= max_val <= 2147483647:
            return 'INT', None
        else:
            return 'BIGINT', None
            
    elif dtype.startswith('float'):
        return 'DOUBLE', None
    
    # For datetime columns
    elif dtype.startswith('datetime'):
        return 'DATETIME', None
    
    # For boolean columns
    elif dtype == 'bool':
        return 'BOOLEAN', None
    
    # For string/object columns
    else:
        max_length = series.astype(str).str.len().max()
        # Add 20% buffer for safety
        suggested_length = min(int(max_length * 1.2), 65535)
        return 'VARCHAR', suggested_length

def read_data_file(file_path, file_extension):
    try:
        if file_extension == 'csv':
            return pd.read_csv(file_path)
        elif file_extension in ('xlsx', 'xls'):
            return pd.read_excel(file_path)
        elif file_extension == 'json':
            return pd.read_json(file_path)
        elif file_extension == 'txt':
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
        
        columns = []
        for col_name in df.columns:
            series = df[col_name]
            mysql_type, length = detect_mysql_type(series)
            
            if length:
                column_def = f"`{col_name}` {mysql_type}({length})"
            else:
                column_def = f"`{col_name}` {mysql_type}"
            
            columns.append(column_def)
        
        create_table_sql += ", ".join(columns) + ")"
        
        # Execute CREATE TABLE
        cursor = connection.cursor()
        cursor.execute(create_table_sql)
        
        # Insert data - handle NULL values properly
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_sql = f"INSERT INTO `{table_name}` ({', '.join([f'`{col}`' for col in df.columns])}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples with proper NULL handling
        data = []
        for _, row in df.iterrows():
            row_data = []
            for val in row:
                if pd.isna(val):
                    row_data.append(None)
                else:
                    # Convert numpy types to Python native types
                    if isinstance(val, (np.integer, np.floating)):
                        row_data.append(val.item())
                    else:
                        row_data.append(val)
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

def analyze_file(file_path, file_extension):
    """Analyze a file and return column statistics"""
    df = read_data_file(file_path, file_extension)
    
    analysis = {}
    for col in df.columns:
        series = df[col]
        dtype = str(series.dtype)
        mysql_type, length = detect_mysql_type(series)
        
        stats = {
            'pandas_type': dtype,
            'mysql_type': f"{mysql_type}({length})" if length else mysql_type,
            'count': len(series),
            'unique': series.nunique(),
            'nulls': series.isna().sum(),
        }
        
        if dtype.startswith(('int', 'float')):
            stats.update({
                'min': series.min(),
                'max': series.max(),
                'mean': series.mean(),
            })
        elif dtype == 'bool':
            stats['true_count'] = series.sum()
            stats['false_count'] = len(series) - series.sum()
        
        analysis[col] = stats
    
    return analysis

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

@app.route('/analyze', methods=['POST'])
def analyze():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        file_extension = filename.rsplit('.', 1)[1].lower()
        
        try:
            analysis = analyze_file(file_path, file_extension)
            return jsonify({
                'status': 'success',
                'analysis': analysis,
            })
        except Exception as e:
            return jsonify({'error': str(e)}), 500
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)
    else:
        return jsonify({'error': 'File type not allowed'}), 400

@app.route('/success')
def success():
    table_name = request.args.get('table_name')
    record_count = request.args.get('record_count')
    return render_template('success.html', table_name=table_name, record_count=record_count)

if __name__ == '__main__':
    app.run(debug=True)