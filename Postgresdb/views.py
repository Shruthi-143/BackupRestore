import os
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from dotenv import load_dotenv
from .utils import *
import psycopg2

load_dotenv()

POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT")
DATABASE_NAME = os.environ.get("DATABASE_NAME")

class PostgresBackup(APIView):
    def get(self, request):
        conn = psycopg2.connect(
            dbname="postgres",
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        cur = conn.cursor()
        
        # Fetch databases
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        databases = cur.fetchall()
        
        cur.close()
        conn.close()
        
        result = [db[0] for db in databases]
        payload = {
            "status":True,
            "message":"List of Databases",
            "data":result,
            "error":None
        }
        return Response(payload, status=status.HTTP_200_OK)
    
    def post(self, request):
        backupPath = request.data.get("backup_file",None)
        backupType = request.data.get("backup_type",None)
        dbName = request.data.get("database_name",None)
        if backupType.lower() == "server":
            if ServerSchemaBackup(POSTGRES_USER, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_PASSWORD, backupPath):
                path =  ServerDataBackup(POSTGRES_USER, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_PASSWORD, backupPath)
                payload = {
                    "status":True,
                    "message":"Backup successfull.",
                    "filePath":path,
                    "error":None
                }
                return Response(payload, status=status.HTTP_200_OK)
        elif backupType.lower() == "database":
            if DatabaseSchemaBackup(POSTGRES_USER, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_PASSWORD, dbName, backupPath):
                payload = {
                    "status":True,
                    "message":"Backup successfull",
                    "filePath":backupPath,
                    "error":None
                }
                return Response(payload, status=status.HTTP_200_OK)

class PostgresRestore(APIView):
    def post(self, request):
        filePath = request.data.get("file_path",None)
        schemaPath = request.data.get("schema_path",None)
        # Restore
        if filePath and schemaPath:
            if ServerSchemaRestore(POSTGRES_USER, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_PASSWORD, schemaPath):
                path = ServerDataRestore(POSTGRES_USER, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_PASSWORD, filePath)
                if path:
                    payload = {
                        "status":True,
                        "message":"Server restored successfully from path.",
                        "filePath":path,
                        "error":None
                    }
                    return Response(payload, status=status.HTTP_200_OK)
                else:
                    payload = {
                        "status": False,
                        "message": "Data restoration failed.",
                        "data": None,
                        "error": "Error restoring data."
                    }
                    return Response(payload, status=status.HTTP_400_BAD_REQUEST)
            else:
                payload = {
                    "status":False,
                    "message":"Server restoration failed.",
                    "data":None,
                    "error":"Error creating databases and schema."
                }
                return Response(payload, status=status.HTTP_400_BAD_REQUEST)
        else:
            payload = {
                "status":False,
                "message":"User has not provided schema and backup file path.",
                "data":None,
                "error":"Restoration will not proceed."
            }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)

class CaseMMRestoreSchema(APIView):
    def post(self, request):
        schemaFilePath = request.data.get("schema_path",None)
        dbName = request.data.get("database_name",None)
        
        if schemaFilePath:
            dbname = RestoreSchema(POSTGRES_USER, POSTGRES_HOST, POSTGRES_PORT, dbName, POSTGRES_PASSWORD, schemaFilePath)
            payload = {
                "status":True,
                "message":"Schema Restored Successfully",
                "dbname":dbname,
                "error":None
            }
            return Response(payload, status=status.HTTP_200_OK)
        else:
            payload = {
                "status":False,
                "message":"Schema Path not provided.",
                "data":None,
                "error":"Schema restoration will not proceed."
            }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)
