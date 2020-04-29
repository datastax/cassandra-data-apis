package testutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	. "github.com/onsi/gomega"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

const baseUrl = `http://localhost:8187`

var cmd *exec.Cmd

const SimulacronStartIp = "127.0.0.101"

func StartSimulacron() {
	if cmd != nil {
		panic("Can not start simulacron multiple times")
	}
	fmt.Println("Starting simulacron")
	cmdStr := fmt.Sprintf("java -jar %s --ip %s", simulacronPath(), SimulacronStartIp)
	cmd = exec.Command("bash", "-c", cmdStr)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Start(); err != nil {
		panic(err)
	}

	started := false
	for i := 0; i < 100; i++ {
		if strings.Contains(out.String(), "Started HTTP server interface") {
			started = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !started {
		panic("Simulacron failed to start")
	}
}

func simulacronPath() string {
	jarPath := os.Getenv("SIMULACRON_PATH")
	if jarPath == "" {
		panic("SIMULACRON_PATH env var is not set, it should point to the simulacron jar file")
	}
	return jarPath
}

func StopSimulacron() {
	if err := cmd.Process.Kill(); err != nil {
		fmt.Println("Failed to kill simulacron ", err)
	}
}

func CreateSimulacronCluster(dc1Length int, dc2Length int) {
	urlPath := `/cluster?data_centers=%d,%d&cassandra_version=3.11.6&name=test_cluster&activity_log=true&num_tokens=1`
	url := baseUrl + fmt.Sprintf(urlPath, dc1Length, dc2Length)
	_, err := http.Post(url, "application/json ", bytes.NewBufferString(""))
	Expect(err).NotTo(HaveOccurred())
}

func GetQueryLogs(dcIndex int) ClusterQueryLogReport {
	urlPath := `/log/0/%d`
	url := baseUrl + fmt.Sprintf(urlPath, dcIndex)
	r, err := http.Get(url)
	Expect(err).NotTo(HaveOccurred())
	var log ClusterQueryLogReport
	err = json.NewDecoder(r.Body).Decode(&log)
	Expect(err).NotTo(HaveOccurred())
	return log
}

func CountLogMatches(nodeLogs []NodeQueryLogReport, query string) QueryMatches {
	matches := QueryMatches{}
	for _, node := range nodeLogs {
		for _, q := range node.Queries {
			message := q.Frame.Message
			if message.Type == "PREPARE" && message.Query == query {
				matches.Prepare++
			} else if message.Type == "EXECUTE" {
				matches.Execute++
			}
		}
	}
	return matches
}

type QueryMatches struct {
	Prepare int
	Execute int
}

type ClusterQueryLogReport struct {
	Id          int64                      `json:"id"`
	DataCenters []DataCenterQueryLogReport `json:"data_centers"`
}

type DataCenterQueryLogReport struct {
	Id    int64                `json:"id"`
	Nodes []NodeQueryLogReport `json:"nodes"`
}

type NodeQueryLogReport struct {
	Id      int64      `json:"id"`
	Queries []QueryLog `json:"queries"`
}

type QueryLog struct {
	Connection string `json:"connection"`
	Frame      Frame  `json:"frame"`
}

type Frame struct {
	ProtocolVersion int     `json:"protocol_version"`
	Message         Message `json:"message"`
}

type Message struct {
	Type  string `json:"type"`
	Query string `json:"query"`
}
