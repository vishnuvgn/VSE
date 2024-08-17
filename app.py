from flask import Flask, jsonify, render_template, request, redirect, url_for, session
import art, sql, update
from celery import Celery


app = Flask(__name__)
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

app.secret_key = '7fdd922ef3de1581de51a04f8c4b6c150b5700dc410b3f197264e430a451da61'
users = {'dante': 'alighieri'}

def login_required(f):
    def wrapper(*args, **kwargs):
        if 'logged_in' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    wrapper.__name__ = f.__name__
    return wrapper

def add_log_entry(message):
    if 'logs' not in session:
        session['logs'] = []
    session['logs'].append(message)


def clear_session():
    for key in list(session.keys()):
        session.pop(key, None)

@celery.task
def celery_sync_tables(tables_to_sync, credentials):
    try:
        airtables = []
        baseId = sql.getBaseId(tables_to_sync[0], credentials)
        atpat = sql.getATPAT(tables_to_sync[0], credentials)
        for table in tables_to_sync: # sent as sql table names
            airtable = sql.getAirtable(table, credentials)
            airtables.append(airtable)
        print(airtables)
        update.updateTables(airtables, baseId, atpat, credentials)
    except Exception as e:
        return f'{e}'
    return 'done'

@app.route('/')
def home():
    return render_template('index.html')


from flask import request, render_template, session

@app.route('/load_database', methods=['POST'])
def load_database():
    credentials = {
        'host': request.form['host'],
        'database': request.form['database'],
        'user': request.form['user'],
        'password': request.form['password'],
        'schema': request.form['schema']
    }
    if sql.testConnection(credentials) == 0: # failed to connect
        add_log_entry('failed to connect to database lol')
        return render_template('dashboard.html', database_loaded=False, credentials=credentials, logs=session.get('logs', []))

    sql.create_audit_table(credentials) # if an audit table doesn't exist, create it
    database={'databaseName': credentials['database'], 'user': credentials['user']}
    session['database'] = database
    add_log_entry('database loaded successfully, good job')

    table_info = sql.listTables(credentials) # nested dict
    nativeTables = table_info['nativeTables']
    junctionTables = table_info['junctionTables']

    
    session['credentials'] = credentials
    session['database_loaded'] = True 
    
    return render_template('dashboard.html', nativeTables=nativeTables, junctionTables=junctionTables, credentials=credentials, database_loaded=True, database=database, logs=session.get('logs', []))


@app.route('/art')
def art_route():
    return jsonify(art.ascii_art_pics)

@app.route('/yewhoenterhere', methods=['GET', 'POST'])
def login():
    clear_session()    
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if username in users and users[username] == password:
            session['logged_in'] = True
            return redirect(url_for('dashboard'))
        else:
            return redirect(url_for('cat'))
    return render_template('login.html')


@app.route('/abandonallhope', methods=['GET', 'POST'])
@login_required
def dashboard():
    database_loaded = session.get('database_loaded', False)
    database = session.get('database', {})
    logs = session.get('logs', [])
    credentials=session.get('credentials', {})
    if not database_loaded:
        return render_template('dashboard.html', database_loaded=database_loaded, database=database, logs=logs)
    table_info = sql.listTables(credentials) # nested dict
    nativeTables = table_info['nativeTables']
    junctionTables = table_info['junctionTables']
    return render_template('dashboard.html', nativeTables=nativeTables, junctionTables=junctionTables, credentials=credentials, database_loaded=database_loaded, database=database, logs=logs)

@app.route('/add_table', methods=['POST'])
@login_required
def add_table():
    base_id = request.form.get('baseId')
    atpat = request.form.get('ATPAT')
    table_names = request.form.getlist('tableNames')  # list of strings
    table_fields_strings = request.form.getlist('tableFields') # list of strings
    
    # convert list of strings to list of lists for table fields
    for i in range(len(table_fields_strings)):
        table_fields_strings[i] = table_fields_strings[i].split(',')
    
    credentials = session.get('credentials', {})
    table_fields_dict = dict(zip(table_names, table_fields_strings))
    try:
        sql.createTables(table_fields_dict, base_id, atpat, credentials)
    except Exception as e:
        add_log_entry(f"couldnt add tables, smth about: {e}")
        return render_template('dashboard.html', logs=session.get('logs', []))
    
    add_log_entry('tables added successfully, you can follow basic instructions, good job')

    table_info = sql.listTables(credentials) # nested dict
    nativeTables = table_info['nativeTables']
    junctionTables = table_info['junctionTables']

    return render_template('dashboard.html', nativeTables=nativeTables, junctionTables=junctionTables, logs=session.get('logs', []))


@app.route('/sync_tables', methods=['POST'])
@login_required
def sync_tables():
    session['syncing'] = True
    credentials = session.get('credentials', {})
    tables_to_sync = request.form.getlist('tables_to_sync')
    
    task = celery_sync_tables.apply_async(args=[tables_to_sync, credentials])
    add_log_entry('syncing tables, you are doing great, just breathe')
    
    table_info = sql.listTables(credentials)
    nativeTables = table_info['nativeTables']
    junctionTables = table_info['junctionTables']

    return render_template('dashboard.html', nativeTables=nativeTables, junctionTables=junctionTables, logs=session.get('logs', []), task_id=task.id, syncing=True)

@app.route('/delete_tables', methods=['POST'])
@login_required
def delete_tables():
    table_names = request.form.getlist('tables_to_delete')
    credentials = session.get('credentials', {})
    try:
        for table in table_names:
            sql.deleteTable(table, credentials)
    except Exception as e:
        add_log_entry(f'failed to delete tables, smth about: {e}')
        return render_template('dashboard.html', logs=session.get('logs', []))
    
    add_log_entry('tables deleted successfully')
    table_info = sql.listTables(credentials)
    nativeTables = table_info['nativeTables']
    junctionTables = table_info['junctionTables']

    return render_template('dashboard.html', nativeTables=nativeTables, junctionTables=junctionTables, logs=session.get('logs', []))

@app.route('/clear_tables', methods=['POST'])
@login_required
def clear_tables():
    table_names = request.form.getlist('tables_to_clear')
    credentials = session.get('credentials', {})
    try:
        for table in table_names:
            sql.clearTable(table, credentials)
    except Exception as e:
        add_log_entry(f'failed to clear tables, smth about: {e}')
        return render_template('dashboard.html', logs=session.get('logs', []))
    
    add_log_entry('tables cleared successfully')
    table_info = sql.listTables(credentials)
    nativeTables = table_info['nativeTables']
    junctionTables = table_info['junctionTables']

    return render_template('dashboard.html', nativeTables=nativeTables, junctionTables=junctionTables, logs=session.get('logs', []))




@app.route('/task_status/<task_id>')
@login_required
def task_status(task_id):
    try:
        task = celery.AsyncResult(task_id)
        if task.state == 'PENDING':
            session['syncing'] = True
            response = {'state': task.state, 'status': 'Pending...'}
        elif task.state == 'SUCCESS':
            session['syncing'] = False
            response = {'state': task.state, 'status': task.result}
            add_log_entry('tables synced successfully, you are a hero')
        elif task.state == 'FAILURE':
            session['syncing'] = False
            response = {'state': task.state, 'status': str(task.info)}  # .info contains the error message
            add_log_entry(f'failed to sync tables, smth about: {task.info}')
        
    except Exception as e:
        session['syncing'] = False
        response = {'state': 'FAILURE', 'status': str(e)}
        add_log_entry(f'failed to sync tables, smth about: {response["status"]}')
        
    return jsonify(response)

@app.route('/cat')
def cat():
    return render_template('cat.html')

@app.route('/exeunt')
@login_required
def logout():
    clear_session()
    return render_template('logout.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
    # app.run(debug=True)