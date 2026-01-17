"""
PyMiniDB Web Application - Flask web interface for the database.
"""
from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
import json
import os
import sys
from pathlib import Path

# Create an absolute path to the project root
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# If app.py is inside a 'web_app' folder, move up one level to find 'src'
PROJECT_ROOT = os.path.dirname(BASE_DIR) 

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from src.catalog.catalog import Catalog
from src.storage.storage_manager import StorageManager
from src.executor.query_executor import QueryExecutor, QueryType
from src.parser.sql_interface import SimpleSQLParser
from src.catalog.schema import ColumnSchema, DataType, ColumnConstraint

app = Flask(__name__)
app.secret_key = 'pyminidb-secret-key-2024'

# Ensure the database file is saved in the project root with an absolute path
DB_PATH = os.path.join(PROJECT_ROOT, 'webapp_database.db')

class WebDatabase:
    """Wrapper for our database for web operations."""
    
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.catalog = Catalog()
        self.storage = StorageManager(db_path)
        self._initialize_database()
        self.executor = QueryExecutor(self.catalog, self.storage)
        self.parser = SimpleSQLParser(self.executor)

        
        
        
    def _initialize_database(self):
        """Initialize or load database."""
        if os.path.exists(self.db_path):
            try:
                self.storage.open()
                print(f"Web app loaded database: {self.db_path}")
            except Exception as e:
                print(f"Error loading database: {e}")
                print("Creating new database...")
                self.storage.create_database()
                self.storage.open()
        else:
            print(f"Creating new database for web app: {self.db_path}")
            self.storage.create_database()
            self.storage.open()
        
        # Create default tables if they don't exist
        self._create_default_tables()
    
    def _create_default_tables(self):
        """Create default tables for the web app."""
        try:
            # Check if tasks table exists
            tables = list(self.catalog.tables.keys())
            if 'tasks' not in tables:
                # Use executor to create table
                result = self.executor.execute(
                    QueryType.CREATE_TABLE,
                    table_name='tasks',
                    columns=[
                        ColumnSchema("id", DataType.INTEGER, [ColumnConstraint.PRIMARY_KEY]),
                        ColumnSchema("title", DataType.VARCHAR, [ColumnConstraint.NOT_NULL], length=200),
                        ColumnSchema("description", DataType.TEXT),
                        ColumnSchema("status", DataType.VARCHAR, length=20),
                        ColumnSchema("priority", DataType.INTEGER),
                        ColumnSchema("due_date", DataType.DATE),
                        ColumnSchema("created_at", DataType.TIMESTAMP),
                    ]
                )
                if result.success:
                    print("Created default 'tasks' table")
                else:
                    print(f"Error creating tasks table: {result.message}")
            
            if 'users' not in tables:
                result = self.executor.execute(
                    QueryType.CREATE_TABLE,
                    table_name='users',
                    columns=[
                        ColumnSchema("id", DataType.INTEGER, [ColumnConstraint.PRIMARY_KEY]),
                        ColumnSchema("username", DataType.VARCHAR, [ColumnConstraint.UNIQUE, ColumnConstraint.NOT_NULL], length=50),
                        ColumnSchema("email", DataType.VARCHAR, [ColumnConstraint.UNIQUE], length=100),
                        ColumnSchema("full_name", DataType.VARCHAR, length=100),
                        ColumnSchema("created_at", DataType.TIMESTAMP),
                    ]
                )
                if result.success:
                    print("Created default 'users' table")
                    
                    # Add some sample users using parser (not executor directly)
                    sample_users = [
                        (1, 'alice', 'alice@example.com', 'Alice Johnson'),
                        (2, 'bob', 'bob@example.com', 'Bob Smith'),
                        (3, 'charlie', 'charlie@example.com', 'Charlie Brown'),
                    ]
                    
                    for user in sample_users:
                        result = self.parser.parse_execute(
                            f"INSERT INTO users VALUES ({user[0]}, '{user[1]}', '{user[2]}', '{user[3]}', '2024-01-01')"
                        )
                        if not result.success:
                            print(f"Error inserting user {user[1]}: {result.message}")
                else:
                    print(f"Error creating users table: {result.message}")
        
        except Exception as e:
            print(f"Error creating default tables: {e}")
    
    def execute_sql(self, sql: str):
        """Execute SQL and return result."""
        return self.parser.parse_execute(sql)
    
    def get_tables(self):
        """Get list of all tables."""
        return list(self.catalog.tables.keys())
    
    def get_table_info(self, table_name: str):
        """Get information about a table."""
        try:
            table = self.catalog.get_table(table_name)
            
            # Get row count
            result = self.execute_sql(f"SELECT COUNT(*) as count FROM {table_name}")
            row_count = 0
            if result.success and result.data:
                row_count = result.data[0].get('count', 0)
            
            # Get indexes count
            indexes = self.catalog.get_table_indexes(table_name)
            
            return {
                'name': table.table_name,
                'columns': [
                    {
                        'name': col_name,
                        'type': col_schema.data_type.value,
                        'length': col_schema.length,
                        'is_pk': col_schema.is_primary_key,
                        'is_unique': col_schema.is_unique,
                        'is_not_null': col_schema.is_not_null
                    }
                    for col_name, col_schema in table.columns.items()
                ],
                'primary_key': table.primary_key,
                'row_count': row_count,
                'indexes': len(indexes)
            }
        except Exception as e:
            return {'error': str(e)}
    
    def get_table_data(self, table_name: str, limit: int = 100):
        """Get data from a table."""
        result = self.execute_sql(f"SELECT * FROM {table_name} LIMIT {limit}")
        if result.success:
            return result.data
        return []
    
    def close(self):
        """Close the database."""
        self.storage.close()

# Initialize database
db = WebDatabase()

@app.route('/')
def index():
    """Home page."""
    tables = db.get_tables()
    return render_template('index.html', tables=tables, db=db)

@app.route('/tables')
def tables():
    """List all tables."""
    tables_info = []
    for table_name in db.get_tables():
        info = db.get_table_info(table_name)
        if 'error' not in info:
            tables_info.append(info)
    
    return render_template('tables.html', tables=tables_info, db=db)

@app.route('/table/<table_name>')
def table_view(table_name):
    """View table data."""
    try:
        table_info = db.get_table_info(table_name)
        if 'error' in table_info:
            flash(f"Error: {table_info['error']}", 'danger')
            return redirect(url_for('tables'))
        
        data = db.get_table_data(table_name)
        return render_template('table_view.html', 
                             table=table_info, 
                             data=data,
                             table_name=table_name,
                             db=db)
    except Exception as e:
        flash(f"Error: {str(e)}", 'danger')
        return redirect(url_for('tables'))

@app.route('/query', methods=['GET', 'POST'])
def query():
    """Execute SQL queries."""
    if request.method == 'POST':
        sql = request.form.get('sql', '').strip()
        if sql:
            result = db.execute_sql(sql)
            
            # Convert result to serializable format
            if result.data:
                # Ensure all values are JSON serializable
                serializable_data = []
                for row in result.data:
                    serializable_row = {}
                    for key, value in row.items():
                        # Convert non-serializable types
                        if hasattr(value, 'isoformat'):  # Date/DateTime
                            serializable_row[key] = value.isoformat()
                        else:
                            serializable_row[key] = value
                    serializable_data.append(serializable_row)
                result_data = serializable_data
            else:
                result_data = None
            
            return jsonify({
                'success': result.success,
                'message': result.message,
                'data': result_data,
                'rows_affected': result.rows_affected,
                'execution_time': result.execution_time
            })
    
    return render_template('query.html', db=db)

@app.route('/api/tables', methods=['GET'])
def api_tables():
    """API endpoint to get all tables."""
    tables = db.get_tables()
    return jsonify({'tables': tables})

@app.route('/api/table/<table_name>', methods=['GET'])
def api_table_data(table_name):
    """API endpoint to get table data."""
    try:
        data = db.get_table_data(table_name)
        table_info = db.get_table_info(table_name)
        return jsonify({
            'success': True,
            'table': table_info,
            'data': data
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/query', methods=['POST'])
def api_query():
    """API endpoint to execute SQL."""
    data = request.get_json()
    sql = data.get('sql', '').strip()
    
    if not sql:
        return jsonify({'success': False, 'error': 'No SQL provided'})
    
    result = db.execute_sql(sql)
    
    # Convert to JSON serializable format
    response = {
        'success': result.success,
        'message': result.message,
        'rows_affected': result.rows_affected,
        'execution_time': result.execution_time
    }
    
    if result.data:
        serializable_data = []
        for row in result.data:
            serializable_row = {}
            for key, value in row.items():
                if hasattr(value, 'isoformat'):
                    serializable_row[key] = value.isoformat()
                else:
                    serializable_row[key] = value
            serializable_data.append(serializable_row)
        response['data'] = serializable_data
    
    return jsonify(response)

# Task Manager Routes (CRUD operations example)

@app.route('/tasks')
def tasks():
    """Task manager page."""
    result = db.execute_sql("SELECT * FROM tasks ORDER BY id DESC")
    tasks_data = result.data if result.success else []
    
    # Get status counts for dashboard
    status_counts = {}
    if tasks_data:
        for task in tasks_data:
            status = task.get('status', 'pending')
            status_counts[status] = status_counts.get(status, 0) + 1
    
    return render_template('tasks.html', 
                         tasks=tasks_data,
                         status_counts=status_counts,
                         db=db)

@app.route('/api/tasks', methods=['GET', 'POST'])
def api_tasks():
    """API endpoint for tasks."""
    if request.method == 'GET':
        # Get tasks with optional filters
        status = request.args.get('status')
        priority = request.args.get('priority')
        
        sql = "SELECT * FROM tasks"
        conditions = []
        
        if status:
            conditions.append(f"status = '{status}'")
        if priority:
            conditions.append(f"priority = {priority}")
        
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        
        sql += " ORDER BY id DESC"
        
        result = db.execute_sql(sql)
        if result.success:
            return jsonify({'success': True, 'tasks': result.data})
        else:
            return jsonify({'success': False, 'error': result.message})
    
    elif request.method == 'POST':
        # Create new task
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'})
        
        title = data.get('title', '').strip()
        description = data.get('description', '').strip()
        status = data.get('status', 'pending')
        priority = data.get('priority', 3)
        due_date = data.get('due_date')
        
        if not title:
            return jsonify({'success': False, 'error': 'Title is required'})
        
        # Build SQL
        columns = ['title', 'description', 'status', 'priority']
        values = [f"'{title}'", f"'{description}'", f"'{status}'", str(priority)]
        
        if due_date:
            columns.append('due_date')
            values.append(f"'{due_date}'")
        
        sql = f"INSERT INTO tasks ({', '.join(columns)}) VALUES ({', '.join(values)})"
        
        result = db.execute_sql(sql)
        return jsonify({
            'success': result.success,
            'message': result.message,
            'rows_affected': result.rows_affected
        })

@app.route('/api/tasks/<int:task_id>', methods=['GET', 'PUT', 'DELETE'])
def api_task(task_id):
    """API endpoint for a specific task."""
    if request.method == 'GET':
        result = db.execute_sql(f"SELECT * FROM tasks WHERE id = {task_id}")
        if result.success and result.data:
            return jsonify({'success': True, 'task': result.data[0]})
        else:
            return jsonify({'success': False, 'error': 'Task not found'})
    
    elif request.method == 'PUT':
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'})
        
        updates = []
        for key, value in data.items():
            if key != 'id':
                if isinstance(value, str):
                    updates.append(f"{key} = '{value}'")
                else:
                    updates.append(f"{key} = {value}")
        
        if not updates:
            return jsonify({'success': False, 'error': 'No updates provided'})
        
        sql = f"UPDATE tasks SET {', '.join(updates)} WHERE id = {task_id}"
        result = db.execute_sql(sql)
        
        return jsonify({
            'success': result.success,
            'message': result.message,
            'rows_affected': result.rows_affected
        })
    
    elif request.method == 'DELETE':
        sql = f"DELETE FROM tasks WHERE id = {task_id}"
        result = db.execute_sql(sql)
        
        return jsonify({
            'success': result.success,
            'message': result.message,
            'rows_affected': result.rows_affected
        })

@app.route('/dashboard')
def dashboard():
    """Database dashboard."""
    tables = db.get_tables()
    
    # Get table statistics
    table_stats = []
    for table_name in tables:
        info = db.get_table_info(table_name)
        if 'error' not in info:
            result = db.execute_sql(f"SELECT COUNT(*) as count FROM {table_name}")
            if result.success and result.data:
                row_count = result.data[0].get('count', 0)
            else:
                row_count = 0
            
            table_stats.append({
                'name': table_name,
                'columns': len(info['columns']),
                'rows': row_count
            })
    
    # Get recent queries (simulated - in production you'd log these)
    recent_queries = [
        {"sql": "SELECT * FROM tasks", "time": "0.002s"},
        {"sql": "INSERT INTO tasks (title) VALUES ('Test')", "time": "0.001s"},
        {"sql": "CREATE TABLE users (id INT PRIMARY KEY)", "time": "0.005s"},
    ]
    
    return render_template('dashboard.html',
                         table_stats=table_stats,
                         recent_queries=recent_queries,
                         total_tables=len(tables),
                         db=db)

@app.route('/create-table', methods=['GET', 'POST'])
def create_table():
    """Create a new table via web interface."""
    if request.method == 'POST':
        table_name = request.form.get('table_name', '').strip()
        columns_text = request.form.get('columns', '').strip()
        
        if not table_name:
            flash('Table name is required', 'danger')
            return render_template('create_table.html', db=db)
        
        if not columns_text:
            flash('Column definitions are required', 'danger')
            return render_template('create_table.html', db=db)
        
        # Parse columns (simple format: name type [constraints])
        columns = []
        lines = columns_text.split('\n')
        
        for line in lines:
            line = line.strip()
            if line:
                parts = line.split()
                if len(parts) >= 2:
                    col_name = parts[0]
                    col_type = parts[1].upper()
                    
                    # Parse constraints
                    constraints = []
                    for constraint in parts[2:]:
                        if constraint.upper() == 'PRIMARY':
                            constraints.append(ColumnConstraint.PRIMARY_KEY)
                        elif constraint.upper() == 'UNIQUE':
                            constraints.append(ColumnConstraint.UNIQUE)
                        elif constraint.upper() == 'NOT':
                            if 'NULL' in parts[parts.index(constraint)+1:]:
                                constraints.append(ColumnConstraint.NOT_NULL)
                    
                    # Map to DataType
                    type_map = {
                        'INTEGER': DataType.INTEGER,
                        'INT': DataType.INTEGER,
                        'VARCHAR': DataType.VARCHAR,
                        'TEXT': DataType.TEXT,
                        'FLOAT': DataType.FLOAT,
                        'DOUBLE': DataType.DOUBLE,
                        'BOOLEAN': DataType.BOOLEAN,
                        'DATE': DataType.DATE,
                        'TIMESTAMP': DataType.TIMESTAMP,
                    }
                    
                    if col_type in type_map:
                        columns.append(ColumnSchema(
                            name=col_name,
                            data_type=type_map[col_type],
                            constraints=constraints
                        ))
        
        if not columns:
            flash('No valid columns defined', 'danger')
            return render_template('create_table.html', db=db)
        
        # Create table
        result = db.executor.execute(
            QueryType.CREATE_TABLE,
            table_name=table_name,
            columns=columns
        )
        
        if result.success:
            flash(f"Table '{table_name}' created successfully!", 'success')
            return redirect(url_for('table_view', table_name=table_name))
        else:
            flash(f"Error creating table: {result.message}", 'danger')
    
    return render_template('create_table.html', db=db)

@app.teardown_appcontext
def shutdown(exception=None):
    """Clean up when the app shuts down."""
    db.close()

# Error handlers
@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html', db=db), 404

@app.errorhandler(500)
def internal_error(e):
    return render_template('500.html', db=db), 500

if __name__ == '__main__':
    # Create necessary directories (using absolute paths)
    os.makedirs(os.path.join(BASE_DIR, 'templates'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, 'static/css'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, 'static/js'), exist_ok=True)
    
    # Get port from environment for deployment, default to 5000 for local
    port = int(os.environ.get('PORT', 5000))
    
    print("\n" + "="*60)
    print("PyMiniDB Web Application")
    print("="*60)
    print(f"Database Path: {DB_PATH}")
    print(f"Starting server on http://0.0.0.0:{port}")
    print("="*60 + "\n")
    
    # Use 0.0.0.0 to allow external access when deployed
    app.run(host='0.0.0.0', port=port, debug=False)
