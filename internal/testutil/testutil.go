package testutil

import (
	"fmt"
	"github.com/gocql/gocql"
	"os"
	"os/exec"
	"path"
	"runtime"
)

var started = false
var session *gocql.Session

func startCassandra() {
	if started {
		return
	}
	started = true
	fmt.Println("Starting Cassandra")
	executeCcm(fmt.Sprintf("create test -v %s -n 1 -s -b", cassandraVersion()))
}

func shutdownCassandra() {
	fmt.Println("Shutting down cassandra")
	executeCcm("remove")
}

func executeCcm(command string) {
	ccmCommand := fmt.Sprintf("ccm %s", command)
	cmd := exec.Command("bash", "-c", ccmCommand)
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

func CreateSchema(filename string) {
	_, currentFile, _, _ := runtime.Caller(0)
	dir := path.Dir(currentFile)
	filePath := path.Join(dir, "schemas", filename)
	executeCcm(fmt.Sprintf("node1 cqlsh -f %s", filePath))
}

func SetupIntegrationTestFixture(queries ...string) *gocql.Session {
	if !IntegrationTestsEnabled() {
		return nil
	}

	startCassandra()

	cluster := gocql.NewCluster("127.0.0.1")

	var err error

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

	if session != nil {
		session.Close()
	}
	shutdownCassandra()
}
