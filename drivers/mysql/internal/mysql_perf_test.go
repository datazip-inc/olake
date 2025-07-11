package driver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake/utils"
	"github.com/docker/docker/api/types/container"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

const (
	FLAG          = true
	mysqlDatabase = "mysql"
	mysqlPassword = "secret1234"
	mysqlUser     = "mysql"
	mysqlHost     = "localhost"
	mysqlPort     = 3306

	installCmd       = "apt-get update && apt-get install -y openjdk-17-jre-headless maven default-mysql-client postgresql postgresql-client iproute2 dnsutils iputils-ping netcat-openbsd nodejs npm jq && npm install -g chalk-cli"
	sourceConfigPath = "/test-olake/drivers/mysql/internal/testconfig/source.json"
)

func TestMySQLPerformance(t *testing.T) {
	ctx := context.Background()

	pwd, err := os.Getwd()
	fmt.Println("Current working directory: ", pwd)
	require.NoErrorf(t, err, "Failed to get current working directory")

	projectRoot := filepath.Join(pwd, "../../..")
	fmt.Println("Project root: ", projectRoot)

	t.Run("Sync", func(t *testing.T) {
		req := testcontainers.ContainerRequest{
			Image: "golang:1.23.2",
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Binds = []string{
					fmt.Sprintf("%s:/mnt/olake:rw", projectRoot),
				}
				hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
			},
			ConfigModifier: func(c *container.Config) {
				c.WorkingDir = "/mnt/olake"
			},
			Env: map[string]string{
				"DEBUG": "false",
			},
			LifecycleHooks: []testcontainers.ContainerLifecycleHooks{
				{
					PostReadies: []testcontainers.ContainerHook{
						func(ctx context.Context, c testcontainers.Container) error {
							if FLAG {
								_, out, err := utils.ExecContainerCommand(ctx, c, "du -csh ./*")
								if err != nil {
									return err
								}
								fmt.Println("ls output: ", string(out))

								_, out, err = utils.ExecContainerCommand(ctx, c, "ls -alh destination")
								if err != nil {
									return err
								}
								fmt.Println("ls output: ", string(out))
								return nil
							}
							//install dependencies
							fmt.Println("Installing dependencies in container")
							if code, _, err := utils.ExecContainerCommand(ctx, c, installCmd); err != nil || code != 0 {
								return err
							}
							fmt.Println("✅ Dependencies installed successfully")

							//connect to mysql
							db, err := sqlx.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", mysqlUser, mysqlPassword, mysqlHost, mysqlPort, mysqlDatabase))
							if err != nil {
								return err
							}
							defer db.Close()
							fmt.Println("✅ Connected to MySQL successfully")

							// create database performance
							if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS performance"); err != nil {
								return err
							}

							// switch to performance database
							if _, err := db.ExecContext(ctx, "USE performance"); err != nil {
								return err
							}
							fmt.Println("✅ Switched to performance database successfully")

							//drop table if exists
							if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS users"); err != nil {
								return err
							}
							fmt.Println("✅ Table dropped successfully")

							//create table
							if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name VARCHAR(255))"); err != nil {
								return err
							}
							fmt.Println("✅ Table created successfully")

							//insert data
							if _, err := db.ExecContext(ctx, "INSERT INTO users (id, name) VALUES (1, 'olake_user')"); err != nil {
								return err
							}
							fmt.Println("✅ Data inserted successfully")

							// perform discover
							fmt.Println("Performing discover in container")
							discoverCmd := fmt.Sprintf("/test-olake/build.sh driver-mysql discover --config %s", sourceConfigPath)
							fmt.Println("Discover command: ", discoverCmd)
							code, output, err := utils.ExecContainerCommand(ctx, c, discoverCmd)
							if err != nil || code != 0 {
								fmt.Println("Discover output: ", string(output))
								return err
							}
							fmt.Println("Discover output: ", string(output))
							fmt.Println("✅ Discover completed successfully")

							// code, out, err := utils.ExecContainerCommand(ctx, container, "cat "+sourceConfigPath)
							// if err != nil || code != 0 {
							// 	return err
							// }
							// fmt.Println("✅ Cat command output: ", string(out))

							return nil
						},
					},
				},
			},
			Cmd: []string{"tail", "-f", "/dev/null"},
		}

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})

		require.NoError(t, err, "Failed to start container")

		defer container.Terminate(ctx)
	})
}
