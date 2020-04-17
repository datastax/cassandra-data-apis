package testutil

import (
	"fmt"
	"github.com/datastax/cassandra-data-apis/log"
	"github.com/gocql/gocql"
	"go.uber.org/zap"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strings"
	"time"
)

var started = false
var session *gocql.Session

func startCassandra() {
	if started {
		return
	}
	started = true
	version := cassandraVersion()
	fmt.Printf("Starting Cassandra %s\n", version)
	executeCcm(fmt.Sprintf("create test -v %s -n 1 -s -b", version))
}

func shutdownCassandra() {
	fmt.Println("Shutting down cassandra")
	executeCcm("remove")
}

func executeCcm(command string) {
	ccmCommand := fmt.Sprintf("ccm %s", command)
	cmd := exec.Command("bash", "-c", ccmCommand)
	output, err := cmd.CombinedOutput()
	outputStr := string(output)
	if outputStr != "" {
		fmt.Println("Output", outputStr)
	}
	if err != nil {
		fmt.Println("Error", err)
		panic(err)
	}
}

func cassandraVersion() string {
	version := os.Getenv("CCM_VERSION")
	if version == "" {
		version = "3.11.6"
	}
	return version
}

func CreateSchema(name string) {
	_, currentFile, _, _ := runtime.Caller(0)
	dir := path.Dir(currentFile)
	filePath := path.Join(dir, "schemas", name, "schema.cql")
	executeCcm(fmt.Sprintf("node1 cqlsh -f %s", filePath))
}

func SetupIntegrationTestFixture(queries ...string) *gocql.Session {
	startCassandra()

	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = cluster.Timeout

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
	if session != nil {
		session.Close()
	}
	shutdownCassandra()
}

func PanicIfError(err error) {
	if err != nil {
		panic(err)
	}
}

func TestLogger() log.Logger {
	if strings.ToUpper(os.Getenv("TEST_TRACE")) == "ON" {
		logger, err := zap.NewProduction()
		if err != nil {
			panic(err)
		}
		return log.NewZapLogger(logger)
	}

	return log.NewZapLogger(zap.NewNop())
}
