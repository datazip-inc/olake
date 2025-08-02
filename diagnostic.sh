#!/bin/bash

echo "=========================================="
echo "Olake Integration Test - Comprehensive Diagnostic"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to check if a container is running
check_container() {
    local container_name=$1
    local service_name=$2
    
    if docker ps --format "table {{.Names}}" | grep -q "^${container_name}$"; then
        echo -e "${GREEN}✓${NC} ${service_name} (${container_name}) is running"
        return 0
    else
        echo -e "${RED}✗${NC} ${service_name} (${container_name}) is NOT running"
        return 1
    fi
}

# Function to check if a port is accessible
check_port() {
    local host=$1
    local port=$2
    local service_name=$3
    
    if nc -z -w5 $host $port 2>/dev/null; then
        echo -e "${GREEN}✓${NC} ${service_name} is accessible on ${host}:${port}"
        return 0
    else
        echo -e "${RED}✗${NC} ${service_name} is NOT accessible on ${host}:${port}"
        return 1
    fi
}

# Function to check Docker resource usage
check_docker_resources() {
    echo -e "\n${YELLOW}Checking Docker resource usage...${NC}"
    
    # Check running containers count
    local container_count=$(docker ps -q | wc -l)
    echo -e "${BLUE}Running containers: ${container_count}${NC}"
    
    if [ "$container_count" -gt 15 ]; then
        echo -e "${RED}⚠ Warning: High number of running containers (${container_count})${NC}"
    fi
    
    # Check Docker system info
    echo -e "${BLUE}Docker system info:${NC}"
    docker system df --format "table {{.Type}}\t{{.TotalCount}}\t{{.Size}}\t{{.Reclaimable}}"
    
    # Check for conflicting containers
    echo -e "\n${YELLOW}Checking for potential port conflicts...${NC}"
    
    # Check for multiple PostgreSQL containers (this is expected - one for source, one for Iceberg)
    local postgres_containers=$(docker ps --format "{{.Names}}" | grep -i postgres | wc -l)
    if [ "$postgres_containers" -eq 2 ]; then
        echo -e "${GREEN}✓${NC} Expected PostgreSQL containers detected (${postgres_containers}):"
        echo -e "${BLUE}  - iceberg-postgres (port 5432) - Iceberg metadata catalog${NC}"
        echo -e "${BLUE}  - olake_postgres-test (port 5433) - PostgreSQL source database${NC}"
        docker ps --format "table {{.Names}}\t{{.Ports}}" | grep -i postgres
    elif [ "$postgres_containers" -gt 2 ]; then
        echo -e "${RED}⚠ Warning: Unexpected number of PostgreSQL containers (${postgres_containers})${NC}"
        docker ps --format "table {{.Names}}\t{{.Ports}}" | grep -i postgres
    else
        echo -e "${YELLOW}⚠ Info: Only ${postgres_containers} PostgreSQL container(s) running${NC}"
    fi
    
    # Check for multiple MySQL containers
    local mysql_containers=$(docker ps --format "{{.Names}}" | grep -i mysql | wc -l)
    if [ "$mysql_containers" -gt 1 ]; then
        echo -e "${RED}⚠ Warning: Multiple MySQL containers detected (${mysql_containers})${NC}"
        docker ps --format "table {{.Names}}\t{{.Ports}}" | grep -i mysql
    fi
    
    # Check for multiple MongoDB containers
    local mongo_containers=$(docker ps --format "{{.Names}}" | grep -i mongo | wc -l)
    if [ "$mongo_containers" -gt 1 ]; then
        echo -e "${RED}⚠ Warning: Multiple MongoDB containers detected (${mongo_containers})${NC}"
        docker ps --format "table {{.Names}}\t{{.Ports}}" | grep -i mongo
    fi
    
    return 0
}

# Function to check network connectivity between test containers
check_network_connectivity() {
    echo -e "\n${YELLOW}Checking network connectivity between test containers...${NC}"
    
    # Get the test container network
    local test_network=$(docker network ls --format "{{.Name}}" | grep -E "(test|olake)" | head -1)
    
    if [ -n "$test_network" ]; then
        echo -e "${BLUE}Test network found: ${test_network}${NC}"
        
        # Check if containers can reach each other
        if docker run --rm --network "$test_network" alpine ping -c 1 iceberg-postgres >/dev/null 2>&1; then
            echo -e "${GREEN}✓${NC} Test containers can reach iceberg-postgres"
        else
            echo -e "${RED}✗${NC} Test containers cannot reach iceberg-postgres"
            echo -e "${YELLOW}  → Services are on 'local-test_iceberg_net' network${NC}"
            echo -e "${YELLOW}  → Test containers are on '${test_network}' network${NC}"
        fi
        
        if docker run --rm --network "$test_network" alpine ping -c 1 minio >/dev/null 2>&1; then
            echo -e "${GREEN}✓${NC} Test containers can reach minio"
        else
            echo -e "${RED}✗${NC} Test containers cannot reach minio"
        fi
        
        if docker run --rm --network "$test_network" alpine ping -c 1 spark-iceberg >/dev/null 2>&1; then
            echo -e "${GREEN}✓${NC} Test containers can reach spark-iceberg"
        else
            echo -e "${RED}✗${NC} Test containers cannot reach spark-iceberg"
        fi
        
        # Check if iceberg network exists
        if docker network ls --format "{{.Name}}" | grep -q "local-test_iceberg_net"; then
            echo -e "${GREEN}✓${NC} Iceberg network (local-test_iceberg_net) exists"
        else
            echo -e "${RED}✗${NC} Iceberg network (local-test_iceberg_net) not found"
        fi
    else
        echo -e "${YELLOW}No test network found, checking default bridge network${NC}"
    fi
}

# Function to check MongoDB connection
check_mongodb() {
    echo -e "\n${YELLOW}Checking MongoDB...${NC}"
    
    # Check if MongoDB container is running
    if ! check_container "primary_mongo" "MongoDB"; then
        return 1
    fi
    
    # Check MongoDB port
    if ! check_port "localhost" "27017" "MongoDB"; then
        return 1
    fi
    
    # Test MongoDB connection with authentication
    echo -e "${YELLOW}Testing MongoDB authentication...${NC}"
    if docker exec primary_mongo mongosh --port 27017 --eval "db.adminCommand('ping')" >/dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} MongoDB authentication working"
    else
        echo -e "${RED}✗${NC} MongoDB authentication failed"
        return 1
    fi
    
    # Test specific user connection
    echo -e "${YELLOW}Testing MongoDB user connection...${NC}"
    if docker exec primary_mongo mongosh "mongodb://mongodb:secure_password123@localhost:27017/olake_mongodb_test?authSource=admin" --eval "db.runCommand({ping: 1})" >/dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} MongoDB user connection working"
    else
        echo -e "${RED}✗${NC} MongoDB user connection failed"
        return 1
    fi
    
    # Test replica set status
    echo -e "${YELLOW}Testing MongoDB replica set status...${NC}"
    if docker exec primary_mongo mongosh "mongodb://admin:password@localhost:27017/admin?replicaSet=rs0" --eval "rs.status().members.forEach(function(member){ if(member.stateStr == 'PRIMARY'){ print('PRIMARY_READY'); }})" 2>/dev/null | grep -q "PRIMARY_READY"; then
        echo -e "${GREEN}✓${NC} MongoDB replica set primary ready"
    else
        echo -e "${RED}✗${NC} MongoDB replica set primary not ready"
        return 1
    fi
    
    # Test replica set health
    echo -e "${YELLOW}Testing MongoDB replica set health...${NC}"
    if docker exec primary_mongo mongosh "mongodb://admin:password@localhost:27017/admin?replicaSet=rs0" --eval "rs.status().ok" 2>/dev/null | grep -q "1"; then
        echo -e "${GREEN}✓${NC} MongoDB replica set healthy"
    else
        echo -e "${RED}✗${NC} MongoDB replica set unhealthy"
        return 1
    fi
    
    return 0
}

# Function to check PostgreSQL
check_postgresql() {
    echo -e "\n${YELLOW}Checking PostgreSQL...${NC}"
    
    # Check if PostgreSQL container is running
    if ! check_container "iceberg-postgres" "PostgreSQL"; then
        return 1
    fi
    
    # Check PostgreSQL port
    if ! check_port "localhost" "5432" "PostgreSQL"; then
        return 1
    fi
    
    # Test PostgreSQL connection
    echo -e "${YELLOW}Testing PostgreSQL connection...${NC}"
    if docker exec iceberg-postgres psql -h localhost -U iceberg -d iceberg -c "SELECT 1;" >/dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} PostgreSQL connection working"
    else
        echo -e "${RED}✗${NC} PostgreSQL connection failed"
        return 1
    fi
    
    return 0
}

# Function to check MinIO
check_minio() {
    echo -e "\n${YELLOW}Checking MinIO...${NC}"
    
    # Check if MinIO container is running
    if ! check_container "minio" "MinIO"; then
        return 1
    fi
    
    # Check MinIO port
    if ! check_port "localhost" "9000" "MinIO"; then
        return 1
    fi
    
    # Test MinIO connection
    echo -e "${YELLOW}Testing MinIO connection...${NC}"
    if docker exec mc mc alias set myminio http://minio:9000 admin password >/dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} MinIO connection working"
    else
        echo -e "${RED}✗${NC} MinIO connection failed"
        return 1
    fi
    
    return 0
}

# Function to check Spark Connect
check_spark_connect() {
    echo -e "\n${YELLOW}Checking Spark Connect...${NC}"
    
    # Check if Spark container is running
    if ! check_container "spark-iceberg" "Spark Connect"; then
        return 1
    fi
    
    # Check Spark Connect port
    if ! check_port "localhost" "15002" "Spark Connect"; then
        return 1
    fi
    
    # Test Spark Connect connection (basic check)
    echo -e "${YELLOW}Testing Spark Connect...${NC}"
    if nc -z localhost 15002 2>/dev/null; then
        echo -e "${GREEN}✓${NC} Spark Connect port accessible"
    else
        echo -e "${RED}✗${NC} Spark Connect not responding"
        return 1
    fi
    
    return 0
}

# Function to check test data files
check_test_files() {
    echo -e "\n${YELLOW}Checking test configuration files...${NC}"
    
    local testdata_dir="drivers/mongodb/internal/testdata"
    
    if [ -f "${testdata_dir}/source.json" ]; then
        echo -e "${GREEN}✓${NC} source.json exists"
    else
        echo -e "${RED}✗${NC} source.json missing"
        return 1
    fi
    
    if [ -f "${testdata_dir}/destination.json" ]; then
        echo -e "${GREEN}✓${NC} destination.json exists"
    else
        echo -e "${RED}✗${NC} destination.json missing"
        return 1
    fi
    
    if [ -f "${testdata_dir}/streams.json" ]; then
        echo -e "${GREEN}✓${NC} streams.json exists"
    else
        echo -e "${RED}✗${NC} streams.json missing"
        return 1
    fi
    
    if [ -f "${testdata_dir}/test_streams.json" ]; then
        echo -e "${GREEN}✓${NC} test_streams.json exists"
    else
        echo -e "${RED}✗${NC} test_streams.json missing"
        return 1
    fi
    
    return 0
}

# Function to check build tools
check_build_tools() {
    echo -e "\n${YELLOW}Checking build tools...${NC}"
    
    if command -v go >/dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} Go is installed"
    else
        echo -e "${RED}✗${NC} Go is not installed"
        return 1
    fi
    
    if command -v mvn >/dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} Maven is installed"
    else
        echo -e "${RED}✗${NC} Maven is not installed"
        return 1
    fi
    
    if command -v java >/dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} Java is installed"
    else
        echo -e "${RED}✗${NC} Java is not installed"
        return 1
    fi
    
    return 0
}

# Function to check system resources
check_system_resources() {
    echo -e "\n${YELLOW}Checking system resources...${NC}"
    
    # Check available memory
    local available_memory=$(free -m | awk 'NR==2{printf "%.0f", $7}')
    echo -e "${BLUE}Available memory: ${available_memory}MB${NC}"
    
    if [ "$available_memory" -lt 2048 ]; then
        echo -e "${RED}⚠ Warning: Low available memory (${available_memory}MB)${NC}"
    fi
    
    # Check available disk space
    local available_disk=$(df -h . | awk 'NR==2{print $4}')
    echo -e "${BLUE}Available disk space: ${available_disk}${NC}"
    
    # Check CPU usage
    local cpu_usage=$(top -l 1 | grep "CPU usage" | awk '{print $3}' | sed 's/%//')
    echo -e "${BLUE}Current CPU usage: ${cpu_usage}%${NC}"
    
    if [ "$cpu_usage" -gt 80 ]; then
        echo -e "${RED}⚠ Warning: High CPU usage (${cpu_usage}%)${NC}"
    fi
}

# Function to provide recommendations
provide_recommendations() {
    echo -e "\n${YELLOW}Recommendations for running full test suite:${NC}"
    echo -e "${BLUE}1. Clean up existing containers:${NC}"
    echo -e "   docker stop \$(docker ps -q) && docker rm \$(docker ps -aq)"
    echo -e ""
    echo -e "${BLUE}2. Start services in order:${NC}"
    echo -e "   docker compose -f ./drivers/mysql/docker-compose.yml up -d && \\"
    echo -e "   docker compose -f ./drivers/postgres/docker-compose.yml up -d && \\"
    echo -e "   docker compose -f ./drivers/mongodb/docker-compose.yml up -d && \\"
    echo -e "   docker compose -f ./destination/iceberg/local-test/docker-compose.yml up minio mc postgres spark-iceberg -d"
    echo -e ""
    echo -e "${BLUE}3. Fix network connectivity (if needed):${NC}"
    echo -e "   # Connect test containers to iceberg network"
    echo -e "   docker network connect local-test_iceberg_net <test-container-name>"
    echo -e ""
    echo -e "${BLUE}4. Run tests with lower parallelism:${NC}"
    echo -e "   go test -v -p 1 ./drivers/mysql/internal/... ./drivers/postgres/internal/... ./drivers/mongodb/internal/..."
    echo -e ""
    echo -e "${BLUE}5. Or run tests individually:${NC}"
    echo -e "   go test -v ./drivers/mysql/internal"
    echo -e "   go test -v ./drivers/postgres/internal"
    echo -e "   go test -v ./drivers/mongodb/internal"
}

# Main diagnostic function
main() {
    local all_good=true
    
    echo "Starting comprehensive diagnostic checks..."
    
    # Check system resources first
    check_system_resources
    
    # Check Docker resources and conflicts
    check_docker_resources
    
    # Check network connectivity
    check_network_connectivity
    
    # Check build tools
    if ! check_build_tools; then
        all_good=false
    fi
    
    # Check test files
    if ! check_test_files; then
        all_good=false
    fi
    
    # Check containers and services
    if ! check_mongodb; then
        all_good=false
    fi
    
    if ! check_postgresql; then
        all_good=false
    fi
    
    if ! check_minio; then
        all_good=false
    fi
    
    if ! check_spark_connect; then
        all_good=false
    fi
    
    echo -e "\n=========================================="
    if [ "$all_good" = true ]; then
        echo -e "${GREEN}✓ All checks passed! Ready to run tests.${NC}"
        echo -e "${GREEN}You can now run: go test -v -p 1 ./drivers/mysql/internal/... ./drivers/postgres/internal/... ./drivers/mongodb/internal/...${NC}"
    else
        echo -e "${RED}✗ Some checks failed. Please fix the issues above before running tests.${NC}"
        provide_recommendations
    fi
    echo "=========================================="
}

# Run the diagnostic
main 