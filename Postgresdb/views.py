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
FILE_PATH = os.environ.get("FILE_PATH")
SCHEMA_FILE_PATH = os.environ.get("SCHEMA_FILE_PATH")

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
        choice = request.data.get("proceed",None)
        backupType = request.data.get("backupType",None)
        if choice and backupType.lower() == "server":
            # Backup
            if ServerSchemaBackup(POSTGRES_USER, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_PASSWORD, FILE_PATH):
                path =  ServerDataBackup(POSTGRES_USER, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_PASSWORD, FILE_PATH)
                payload = {
                    "status":True,
                    "message":"Backup successfull.",
                    "filePath":path,
                    "error":None
                }
                return Response(payload, status=status.HTTP_200_OK)
        # elif choice and backupType.lower() == "database":

class PostgresRestore(APIView):
    def post(self, request):
        choice = request.data.get("proceed",None)
        # Restore
        if choice and choice == True:
            if ServerSchemaRestore(POSTGRES_USER, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_PASSWORD, SCHEMA_FILE_PATH):
                path = ServerDataRestore(POSTGRES_USER, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_PASSWORD, FILE_PATH)
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
                "message":"User has chosed not to proceed.",
                "data":None,
                "error":"Restoration will not proceed."
            }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)

class CaseMMRestoreSchema(APIView):
    def post(self, request):
        choice = request.data.get("proceed",None)
        
        if choice and choice == True:
            dbname = RestoreSchema(POSTGRES_USER, POSTGRES_HOST, POSTGRES_PORT, DATABASE_NAME, POSTGRES_PASSWORD, SCHEMA_FILE_PATH)
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
                "message":"User has chosed not to proceed.",
                "data":None,
                "error":"Schema restoration will not proceed."
            }
            return Response(payload, status=status.HTTP_200_OK)
