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
	"strconv"
	"strings"
	"time"
)

var started = false
var shouldStartCassandra = false
var shouldStartSimulacron = false
var setupQueries = make([]string, 0)
var setupHandlers = make([]func(), 0)
var createdSchemas = make(map[string]bool)
var session *gocql.Session

type commandOptions int

const (
	cmdFatal commandOptions = 1 << iota
	cmdNoError
	cmdNoOutput
)

func (o commandOptions) IsSet(options commandOptions) bool { return o&options != 0 }

const clusterName = "test"

func doesClusterExist(name string) bool {
	output := executeCcm("list", cmdNoOutput)
	nameInUse := "*" + name
	for _, cluster := range strings.Fields(output) {
		if cluster == name || cluster == nameInUse {
			return true
		}
	}
	return false
}

func keepCluster() bool {
	value, _ := strconv.ParseBool(os.Getenv("TEST_KEEP_CLUSTER"))
	return value
}

func startCassandra() bool {
	if started {
		return false
	}
	started = true
	version := cassandraVersion()
	fmt.Printf("Starting Cassandra %s\n", version)

	if !keepCluster() {
		executeCcm("stop --not-gently", cmdNoError|cmdNoOutput)
		executeCcm(fmt.Sprintf("remove %s", clusterName), cmdNoError|cmdNoOutput)
	}

	if !doesClusterExist(clusterName) {
		executeCcm(fmt.Sprintf("create %s -v %s -n 1 -s -b", clusterName, version), cmdFatal)
		return true
	} else {
		executeCcm(fmt.Sprintf("switch %s", clusterName), cmdFatal)
		executeCcm("start", cmdFatal)
		return false
	}
}

func shutdownCassandra() {
	fmt.Println("Shutting down cassandra")
	if !keepCluster() {
		executeCcm(fmt.Sprintf("remove %s", clusterName), 0)
	}
}

func executeCcm(command string, cmdType commandOptions) string {
	ccmCommand := fmt.Sprintf("ccm %s", command)
	cmd := exec.Command("bash", "-c", ccmCommand)
	output, err := cmd.CombinedOutput()
	outputStr := string(output)
	if outputStr != "" && !cmdType.IsSet(cmdNoOutput) {
		fmt.Println("Output", outputStr)
	}
	if err != nil && !cmdType.IsSet(cmdNoError) {
		fmt.Println("Error", err)
		if cmdType.IsSet(cmdFatal) {
			panic(err)
		}
	}
	return outputStr
}

func cassandraVersion() string {
	version := os.Getenv("CCM_VERSION")
	if version == "" {
		version = "3.11.6"
	}
	return version
}

func CreateSchema(name string) {
	if createdSchemas[name] {
		return
	}

	createdSchemas[name] = true
	_, currentFile, _, _ := runtime.Caller(0)
	dir := path.Dir(currentFile)
	filePath := path.Join(dir, "schemas", name, "schema.cql")
	executeCcm(fmt.Sprintf("node1 cqlsh -f %s", filePath), cmdFatal)
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

// Gets the shared session for this suite
func GetSession() *gocql.Session {
	return session
}

func EnsureCcmCluster(setupHandler func(), queries ...string) {
	shouldStartCassandra = true

	if setupHandler != nil {
		setupHandlers = append(setupHandlers, setupHandler)
	}

	for _, query := range queries {
		setupQueries = append(setupQueries, query)
	}
}

func EnsureSimulacronCluster() {
	shouldStartSimulacron = true
}

func BeforeTestSuite() {
	// Start the required process for the suite to run
	if shouldStartCassandra {
		isNew := startCassandra()

		cluster := gocql.NewCluster("127.0.0.1")
		cluster.Timeout = 5 * time.Second
		cluster.ConnectTimeout = cluster.Timeout

		var err error

		if session, err = cluster.CreateSession(); err != nil {
			panic(err)
		}

		if isNew {
			for _, query := range setupQueries {
				err := session.Query(query).Exec()
				PanicIfError(err)
			}

			for _, handler := range setupHandlers {
				handler()
			}
		}
	}

	if shouldStartSimulacron {
		StartSimulacron()
		CreateSimulacronCluster(3, 3)
	}
}

func AfterTestSuite() {
	// Shutdown previously started processes
	if shouldStartCassandra {
		session.Close()
		shutdownCassandra()
	}

	if shouldStartSimulacron {
		StopSimulacron()
	}
}
