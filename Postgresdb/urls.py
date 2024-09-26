from django.urls import path
from .views import *

urlpatterns = [
    path('BackupPostgres/', PostgresBackup.as_view(),name='Postgres-Backup'),
    path('RestorePostgres/',PostgresRestore.as_view(),name='Postgres-Restore'),
    path('SchemaRestore/',CaseMMRestoreSchema.as_view(), name='Schema-Restore'),
]