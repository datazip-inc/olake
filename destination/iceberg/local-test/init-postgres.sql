SELECT 'CREATE DATABASE amoro'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'amoro')\gexec

