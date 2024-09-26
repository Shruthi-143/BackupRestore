import os
import subprocess
import  datetime
from .views import *
import re

# Function to backup server schema
def ServerSchemaBackup( user, host, port, password, filePath):
    os.environ['PGPASSWORD'] = password
    filePath = os.path.join(filePath, f'{datetime.datetime.now().strftime("%d%m%Y")}_{host}_schema.sql')
    temp_filepath = filePath + ".tmp"
    command = [
            'pg_dumpall',
            '-U', user,
            '-h', str(host),
            '-p', str(port),
            '--schema-only',
            '-v', 
            '-f',temp_filepath
            ]
    result = subprocess.run(command,check=True)
    
    with open(temp_filepath, 'r') as infile, open(filePath, 'w') as outfile:
        for line in infile:
            if 'CREATE ROLE postgres' in line or 'ALTER ROLE postgres' in line:
                continue  # Skip lines related to postgres role
            outfile.write(line)

    os.remove(temp_filepath)
    
    if result.returncode != 0:
        print(f"Backup failed: {result.stderr.decode()}")
        return False
    else:
        # print(f"Backup successfull. File saved to {filePath}")
        return True

# Function to backup server data
def ServerDataBackup( user, host, port, password, filePath):
    os.environ['PGPASSWORD'] = password
    backupFIlePath = os.path.join(filePath, f'{datetime.datetime.now().strftime("%d%m%Y")}_{host}_backup.sql')
    command = f"pg_dumpall -U {user} -h {host} -p {port} | grep -v 'CREATE ROLE postgres' | grep -v 'ALTER ROLE postgres' > {backupFIlePath}"

    try:
        result = subprocess.run(command, shell=True, stderr=subprocess.PIPE)

        if result.returncode != 0:
            print(f"Backup failed: {result.stderr.decode()}")
            return False
        else:
            print(f"Backup successfull. File saved to {backupFIlePath}")
            return backupFIlePath
            
    except subprocess.CalledProcessError as e:
        print(f"Backup failed: {e}")
    finally:
        del os.environ['PGPASSWORD']

# Function to restore server schema
def ServerSchemaRestore(user, host, port, password, filePath):
    db_names = set()
    with open(filePath, 'r') as file:
        content = file.read()
        db_names = set(re.findall(r'CREATE\s+DATABASE\s+("([^"]+)"|([^\s]+))\s+WITH\s+', content, re.IGNORECASE))
        db_names = {match[1] if match[1] else match[2] for match in db_names}
        print("Database Names: ",db_names)
    
    for dbName in db_names:
        command = f'CREATE DATABASE \"{dbName}\";'
        print(command)
        os.environ['PGPASSWORD'] = password
        try:
            result = subprocess.run(
                ['psql', '-U', user, '-h', host, '-p', str(port), '-c', command],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            if result.returncode == 0:
                print(f"Database restored successfully from {filePath}")
            else:
                print(f"Restoration failed")
                print(f"Error Output: {result.stderr.decode()}")
                return False
        except subprocess.CalledProcessError as e:
            print(f"Restore failed: {e}")
            return str(e)
    return True
            
# Function to restore server data         
def ServerDataRestore( user, host, port, password, filePath):
    print(filePath)
    os.environ['PGPASSWORD'] = password
    command = [
        'psql',
        '-U', user,
        '-h', host,
        '-p', str(port),
        '-f', filePath
    ]
    try:
        # Run the command
        result = subprocess.run(command, stderr=subprocess.PIPE, check=True)
        
        # Check if the command was successful
        if result.returncode == 0:
            print(f"Server restored successfully from {filePath}")
            return filePath
        else:
            print(f"Restoration failed")
            print(f"Error Output: {result.stderr.decode()}")
            return False
    except subprocess.CalledProcessError as e:
        print(f"Restore failed: {e}")
        return False
    finally:
        del os.environ['PGPASSWORD']


def RestoreSchema(user, host, port, dbname, password, schemaBackupPath):
    os.environ['PGPASSWORD'] = password

    checkDbCommand = [
        'psql',
        '-U', user,
        '-h', host,
        '-p', str(port),
        '-d', 'postgres',  # Connect to default database to check
        '-tAc', f"SELECT 1 FROM pg_database WHERE datname = '{dbname}'"
    ]
    try:
        print(f"Checking if database '{dbname}' exists...")
        result = subprocess.run(checkDbCommand, check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        
        # If result stdout is empty, it means the database doesn't exist
        if not result.stdout.strip():
            raise Exception("Database does not exist")
        print(f"Database '{dbname}' exists.")
    except Exception as e:
        print(f"Database '{dbname}' does not exist, creating database...")
        try:
            createDbCommand = f'CREATE DATABASE "{dbname}";'
            subprocess.run(
                ['psql', '-U', user, '-h', host, '-p', str(port), '-d', 'postgres', '-c', createDbCommand],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            print(f"Database '{dbname}' created successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Error during database creation: {e.stderr}")
            return False
            
    try:
        # Restore schema
        command = [
            'psql',
            '-U', user,
            '-h', host,
            '-p', str(port),
            '-d', dbname,
            '-f', schemaBackupPath
        ]
        subprocess.run(command, check=True)
        print(f"Schema restoration successful for database: {dbname}")
    except subprocess.CalledProcessError as e:
        print(f"Error during schema restoration: {str(e)}")
        return False
    finally:
        os.environ.pop('PGPASSWORD', None)
    
    return dbname


def DatabaseSchemaBackup(user, host, port, password, dbName, filePath):
    os.environ['PGPASSWORD'] = password
    filePath = os.path.join(filePath, f'{datetime.datetime.now().strftime("%d%m%Y")}_{host}_{dbName}_schema.sql')
    tempFilepath = filePath + ".tmp"
    
    command = [
        'pg_dump',
        '-U', user,
        '-h', str(host),
        '-p', str(port),
        '--schema-only',
        '-v', 
        '-f', tempFilepath,
        dbName
    ]
    
    result = subprocess.run(command, check=True)
    
    with open(tempFilepath, 'r') as infile, open(filePath, 'w') as outfile:
        for line in infile:
            if 'CREATE ROLE postgres' in line or 'ALTER ROLE postgres' in line:
                continue  # Skip lines related to postgres role
            outfile.write(line)

    os.remove(tempFilepath)
    
    if result.returncode != 0:
        print(f"Backup failed: {result.stderr.decode()}")
        return False
    else:
        # print(f"Backup successful. File saved to {filePath}")
        return True
