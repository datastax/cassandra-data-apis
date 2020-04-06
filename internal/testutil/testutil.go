package testutil

import (
	"fmt"
	"github.com/gocql/gocql"
	"os"
	"os/exec"
)

var started = false

func startCassandra() {
	if started {
		return
	}
	started = true
	fmt.Println("Starting Cassandra")
	//ccmCmd := "ccm status || true"
	ccmCmd := fmt.Sprintf("ccm create test -v %s -n 1 -s -b", cassandraVersion())
	cmd := exec.Command("bash", "-c", ccmCmd)

	output, err := cmd.CombinedOutput()
	fmt.Println("Output", string(output))
	if err != nil {
		fmt.Println("Error", err)
		panic(err)
	}
}

func shutdownCassandra() {
	fmt.Println("Shutting down cassandra")
	cmd := exec.Command("bash", "-c", "ccm remove")
	output, err := cmd.CombinedOutput()
	fmt.Println("Output", string(output))
	if err != nil {
		fmt.Println("Error", err)
		panic(err)
	}
}

func IntegrationTestsEnabled() bool {
	return os.Getenv("RUN_INTEGRATION_TESTS") == "1"
}

func cassandraVersion() string {
	version := os.Getenv("CCM_VERSION")
	if version == "" {
		version = "3.11.6"
	}
	return version
}

func SetupIntegrationTestFixture(queries ...string) *gocql.Session {
	if !IntegrationTestsEnabled() {
		return nil
	}

	startCassandra()

	cluster := gocql.NewCluster("127.0.0.1")

	var (
		session *gocql.Session
		err     error
	)

	if session, err = cluster.CreateSession(); err != nil {
		panic(err)
	}

	for _, query := range queries {
		err := session.Query(query).Exec()
		if err != nil {
			panic(err)
		}
	}

	return session
}

func TearDownIntegrationTestFixture() {
	if !IntegrationTestsEnabled() {
		return
	}

	shutdownCassandra()
}
