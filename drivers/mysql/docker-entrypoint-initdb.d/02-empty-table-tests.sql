CREATE DATABASE IF NOT EXISTS empty_table_tests;
USE empty_table_tests;

CREATE TABLE IF NOT EXISTS test_with_stats (
    id INT PRIMARY KEY, 
    data VARCHAR(100)
);
INSERT INTO test_with_stats VALUES (1, 'test data 1'), (2, 'test data 2');
ANALYZE TABLE test_with_stats;

CREATE TABLE IF NOT EXISTS test_empty (
    id INT PRIMARY KEY, 
    data VARCHAR(100)
);
ANALYZE TABLE test_empty;

CREATE TABLE IF NOT EXISTS test_small_no_stats (
    id INT PRIMARY KEY, 
    data VARCHAR(100)
);
INSERT INTO test_small_no_stats VALUES (1, 'test data');

CREATE TABLE IF NOT EXISTS test_large_no_stats (
    id INT PRIMARY KEY, 
    data TEXT
);
INSERT INTO test_large_no_stats VALUES 
    (1, REPEAT('x', 1000)),
    (2, REPEAT('y', 1000)),
    (3, REPEAT('z', 1000));
