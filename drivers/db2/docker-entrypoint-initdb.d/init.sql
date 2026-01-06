-- Connect to the test database
CONNECT TO testdb;

-- Create the PUBLIC schema for testing
CREATE SCHEMA PUBLIC AUTHORIZATION db2inst1;

-- Grant necessary permissions
GRANT ALL ON SCHEMA PUBLIC TO db2inst1;
