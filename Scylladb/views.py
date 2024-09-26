from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from cassandra.cluster import Cluster
import os
from dotenv import load_dotenv
from .utils import *


load_dotenv()

END_POINTS = os.environ.get("SCYLLA_ENDPOINTS").split(',')
HOST = os.environ.get("SCYLLA_HOST")
USERNAME = os.environ.get("SCYLLA_USER")
PASSWORD = os.environ.get("SCYLLA_PASSWORD")
KEYSPACE = os.environ.get("SCYLLA_KEYSPACE")

class ScyllaBackup(APIView):
    def get(self, request):
        cluster = Cluster(END_POINTS, port=9042)
        session = cluster.connect()
        keySpaces = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        
        keySpaceNames = []
        for row in keySpaces:
            keySpaceNames.append(row.keyspace_name)

        session.shutdown()
        cluster.shutdown()
        
        payload = {
                "status": True,
                "message": "List of available keyspaces in the cluster",
                "data": keySpaceNames,
                "error": None
            }
        return Response(payload, status=status.HTTP_200_OK)
    
    def post(self,request):
        keySpaceName = request.data.get("keyspace_name", None)
        tableName = request.data.get("table_name", None)
        userInput = request.data.get("choice",None)
        backupPath = request.data.get("backup_path",None)
        
        if not userInput and userInput is None:
            estimatedBackupSize = GetEstimatedBackupSize(HOST, USERNAME, PASSWORD, keySpaceName)
            payload = {
                "status": True,
                "message": "Estimated backup size would be.",
                "data": estimatedBackupSize,
                "error": None
            }
            return Response(payload, status=status.HTTP_200_OK)

        if userInput is not None and userInput.lower()=='yes':
            try:
                snapShotPaths = CaptureDataForSingleTable(HOST, USERNAME, PASSWORD, keySpaceName, tableName, backupPath)
                payload = {
                    "status": True,
                    "message": "Backup done successfully",
                    "data": snapShotPaths,
                    "error": None
                }
                return Response(payload, status=status.HTTP_200_OK)
            except Exception as e:
                payload = {
                    "status": False,
                    "message": "Backup failed due to an error.",
                    "data": None,
                    "error": str(e)
                }
                return Response(payload, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            payload = {
                    "status": False,
                    "message": "Backup not initiated.",
                    "data": None,
                    "error": None
                }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)

class ScyllaKeyspaceAndTable(APIView):
    def get(self, request):
        keySpaceName = request.data.get("keyspace_name", None)
        tableName = request.data.get("table_name", None)
        
        if keySpaceName and tableName:
            if KeyspaceExists(HOST, USERNAME, PASSWORD, keySpaceName):
                if CheckTablesExist(HOST, USERNAME, PASSWORD, keySpaceName, tableName):
                    payload = {
                        "status": True,
                        "message": "Table exists.",
                        "data": tableName,
                        "error": None
                    }
                    return Response(payload, status=status.HTTP_200_OK)
                else:
                    payload = {
                        "status": True,
                        "message": "Table does not exists.",
                        "data": tableName,
                        "error": None
                    }
                    return Response(payload, status=status.HTTP_200_OK)
            else:
                payload = {
                    "status": False,
                    "message": "Keyspace does not exists.",
                    "data": keySpaceName,
                    "error": None
                }
                return Response(payload, status=status.HTTP_400_BAD_REQUEST)
        else:
            payload = {
                "status": False,
                "message": "Keyspace name and table name are required.",
                "data": None,
                "error": None
            }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)

class ScyllaRestoreForSingleTable(APIView):
    def post(self, request):
        backupFile = request.data.get("backup_file", None)
        keyspace = request.data.get("keyspace",None)
        tableName = request.data.get("tablename",None)
        
        try:
            if RestoreDataForSingleTable(HOST, USERNAME, PASSWORD, keyspace, tableName, backupFile):
                payload = {
                    "status": True,
                    "message": f"Restoration of table {tableName} completed successfully. Please restart ScyllaDB to reflect the newly backed-up data.",
                    "data": None,
                    "error": None
                }
                return Response(payload, status=status.HTTP_200_OK)
            else:
                payload = {
                   "status": False,
                    "message": "Restoration failed",
                    "data":None,
                    "error": "Check if the keyspace and table name are correct."
                }
                return Response(payload, status=status.HTTP_200_OK)
        except Exception as e:
                payload = {
                    "status": False,
                    "message": "Restoration failed due to an error.",
                    "data": None,
                    "error": str(e),
                }
                return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def put(self, request):
        choice = request.data.get("proceed",None)
        if choice:
            StartScylla(HOST,USERNAME,PASSWORD)
            payload = {
                    "status": True,
                    "message": "Scylladb has been restarted.",
                    "data": None,
                    "error": None,
                }
            return Response(payload, status=status.HTTP_200_OK)
        
class ScyllaBackupKeyspace(APIView):
    def post(self, request):
        keyspaceName = request.data.get("keyspace_name",None)
        backupPath = request.data.get("backup_path",None)
        if keyspaceName:
            path = CaptureKeySpaceSnapshot(HOST, USERNAME, PASSWORD, keyspaceName, backupPath)
            payload = {
                "status": True,
                "message": "Backup done",
                "data": path,
                "error": None
            }
            return Response(payload, status=status.HTTP_200_OK)
        else:
            payload = {
                "status": False,
                "message": "Backup cannot proceed.",
                "data": None,
                "error": "keyspace not provided."
            }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)

class ScyllaRestoreKeyspace(APIView):
    def post(self, request):
        keyspaceName = request.data.get("keyspace_name",None)
        backupFile = request.data.get("backup_file",None)
        
        RestoreKeySpaceFromLocal(HOST, USERNAME, PASSWORD, keyspaceName, backupFile)
        payload = {
                "status": True,
                "message": f"Restore done for keyspaces{keyspaceName}. Please restart ScyllaDB to reflect the newly backed-up data.",
                "data": "path",
                "error": None
            }
        return Response(payload, status=status.HTTP_200_OK)
    
    def put(self, request):
        choice = request.data.get("proceed",None)
        if choice:
            StartScylla(HOST,USERNAME,PASSWORD)
            payload = {
                    "status": True,
                    "message": "Scylladb has been restarted.",
                    "data": None,
                    "error": None,
                }
            return Response(payload, status=status.HTTP_200_OK)
        
    