from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import os
from dotenv import load_dotenv
from .utils import *


load_dotenv()

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
MINIO_SECURE = os.environ.get("MINIO_SECURE")
MINIO_BUCKET_NAME = os.environ.get("MINIO_BUCKET_NAME")
MINIO_DOWNLOAD_DIR = os.environ.get("MINIO_DOWNLOAD_DIR")

client = InitializeClient(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE)

class BucketList(APIView):
    def get(self, request):
        if client:
            bucketList = ListBuckets(client)
            if bucketList:
                payload = {
                    "status":True,
                    "message":"List of buckets in object store",
                    "data":bucketList,
                    "error":None
                }
                return Response(payload, status=status.HTTP_200_OK)
            else:
                payload = {
                    "status":False,
                    "message":"Error in listing of buckets in object store",
                    "data":None,
                    "error":"Empty bucket cant be listed."
                }
                return Response(payload, status=status.HTTP_400_BAD_REQUEST)
        else:
            payload = {
                    "status":False,
                    "message":"Cant able to connect Minio.",
                    "data":None,
                    "error":"Connection Failed."
                }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)
    
class MinioBackup(APIView):
    def post(self, request):
        bucketName = request.data.get("bucket_name",None)
        backupPath = request.data.get("backup_path",None)
        
        if bucketName and backupPath:
            if DownloadFilesFromBucket(bucketName,backupPath,client):
                payload = {
                    "status":True,
                    "message":"Files from the object store are downloaded succesfully.",
                    "data":backupPath,
                    "error":None
                }
                return Response(payload, status=status.HTTP_200_OK)
            else:
                payload = {
                    "status":False,
                    "message":"Error in downloading files from bucket.",
                    "data":None,
                    "error":"Either path or bucekt name not provided."
                }
                return Response(payload, status=status.HTTP_400_BAD_REQUEST)

class MinioRestore(APIView):
    def post(self, request):
        backupPath = request.data.get("file_path",None)
        bucketName = request.data.get("bucket_name",None)
        if backupPath:
            if UploadFiles(client, bucketName,backupPath):
                payload = {
                    "status":True,
                    "message":"Files restored to object store succesfully from path.",
                    "data":backupPath,
                    "error":None
                }
                return Response(payload, status=status.HTTP_200_OK)
        else:
            payload = {
                    "status":False,
                    "message":"Provide valid backup path.",
                    "data":None,
                    "error":"Backup path not provided by user."
                }
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)
    
        