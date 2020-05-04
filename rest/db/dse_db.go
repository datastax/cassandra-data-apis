package db

import (
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/datastax/cassandra-data-apis/log"
	"regexp"
	"time"

	"github.com/gocql/gocql"

	e "github.com/datastax/cassandra-data-apis/rest/errors"
)

const (
	// ProtoVersion defines the protocol version that the driver will use to communicate with the cluster. By setting to 0
	// the driver will discover the highest supported protocol for the cluster.
	ProtoVersion = 0

	// DSEConnectTimeout is the initial connection timeout used for dialing the server
	DSEConnectTimeout = time.Second * 10

	dataEndpointUsernameEnvVar = "DATA_ENDPOINT_USERNAME"
	dataEndpointPasswordEnvVar = "DATA_ENDPOINT_PASSWORD"
)

type dseDB struct {
	session *gocql.Session
}

// GetDataEndpointCredentials returns the data-endpoint user credentials
func GetDataEndpointCredentials() (string, string, error) {
	return "", "", errors.New("Not implemented")
}

func newDseDB(username string, password string, hosts ...string) (*dseDB, error) {
	cluster := createSession(hosts, username, password)

	var (
		session        *gocql.Session
		err            error
	)
	retryable := func() error {
		session, err = cluster.CreateSession()
		if err != nil {
			log.With("error", err).Error("error from cluster.CreateSession - will retry")
			return err
		}

		return nil
	}

	err = retryable()
	if err != nil {
		return nil, err
	}

	if session == nil {
		return nil, errors.New("failed to create session")
	}

	log.Info("connection to DB successful")

	return &dseDB{
		session: session,
	}, nil
}

func createSession(hosts []string, username string, password string) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(hosts...)

	cluster.ProtoVersion = ProtoVersion
	cluster.ConnectTimeout = DSEConnectTimeout
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: username, Password: password}
	return cluster
}

func (d *dseDB) AuthenticateUser(username string, password string, hosts ...string) error {
	cluster := createSession(hosts, username, password)

	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}

	session.Close()

	return nil
}

func (d *dseDB) GetSession() interface{} {
	return d.session
}

func (d *dseDB) Select(stmt string, username string, values ...interface{}) ([]map[string]interface{}, error) {
	q := d.session.Query(stmt, values...)

	if username != "" {
		q = q.ExecuteAs(username)
	}

	iter := q.Consistency(gocql.LocalQuorum).Iter()

	returned, retErr := iter.SliceMap()
	if retErr != nil {
		return nil, retErr
	}

	return returned, nil
}

func (d *dseDB) SelectWithPaging(stmt string, username string, pageState string, pageSize int, values ...interface{}) ([]map[string]interface{}, string, error) {
	q := d.session.Query(stmt, values...)

	if username != "" {
		q = q.ExecuteAs(username)
	}

	if pageState != "" {
		state, err := base64.StdEncoding.DecodeString(pageState)
		if err != nil {
			return nil, "", err
		}
		q = q.PageState(state)
	} else {
		q = q.PageState(nil)
	}

	if pageSize != 0 {
		q = q.PageSize(pageSize)
	}

	iter := q.Consistency(gocql.LocalQuorum).Iter()

	state := base64.StdEncoding.EncodeToString(iter.PageState())
	returned, retErr := iter.SliceMap()
	if retErr != nil {
		return nil, "", retErr
	}

	return returned, state, nil
}

func (d *dseDB) Insert(stmt string, username string, values ...interface{}) error {
	q := d.session.Query(stmt, values...)

	if username != "" {
		q = q.ExecuteAs(username)
	}

	err := q.Consistency(gocql.LocalQuorum).Exec()
	if err != nil {
		return err
	}

	return nil
}

func (d *dseDB) Update(stmt string, username string, values ...interface{}) error {
	q := d.session.Query(stmt, values...)

	if username != "" {
		q = q.ExecuteAs(username)
	}

	err := q.Consistency(gocql.LocalQuorum).Exec()
	if err != nil {
		return err
	}

	return nil
}

func (d *dseDB) Delete(stmt string, username string, values ...interface{}) error {
	q := d.session.Query(stmt, values...)

	if username != "" {
		q = q.ExecuteAs(username)
	}

	err := q.Consistency(gocql.LocalQuorum).Exec()
	if err != nil {
		return err
	}

	return nil
}

func (d *dseDB) Create(stmt string, username string) error {
	q := d.session.Query(stmt)

	if username != "" {
		q = q.ExecuteAs(username)
	}

	err := q.Consistency(gocql.LocalQuorum).Exec()
	if err != nil {
		return err
	}

	return nil
}

func (d *dseDB) Alter(stmt string, username string) error {
	q := d.session.Query(stmt)

	if username != "" {
		q = q.ExecuteAs(username)
	}

	err := q.Consistency(gocql.LocalQuorum).Exec()
	if err != nil {
		re := regexp.MustCompile("Column with name '.*' already exists")
		if re.MatchString(err.Error()) {
			return e.NewConflictError("column already exists")
		}
		return err
	}

	return nil
}

func (d *dseDB) Drop(stmt string, username string) error {
	q := d.session.Query(stmt)

	if username != "" {
		q = q.ExecuteAs(username)
	}

	err := q.Consistency(gocql.LocalQuorum).Exec()
	if err != nil {
		re := regexp.MustCompile("Table '.*\\..*' doesn't exist")
		if re.MatchString(err.Error()) {
			return e.NewNotFoundError("table not found")
		}
		return err
	}

	return nil
}

func (d *dseDB) DescribeKeyspace(keyspace, username string) (*gocql.KeyspaceMetadata, error) {
	// Query system_schema first to make sure user is authorized
	stmt := "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?"
	rows, retErr := d.Select(stmt, username, keyspace)
	if retErr != nil {
		return nil, retErr
	}

	if len(rows) == 0 {
		return nil, e.NewNotFoundError(fmt.Sprintf("keyspace %s not found", keyspace))
	}

	keyspaceMetadata, retErr := d.session.KeyspaceMetadata(keyspace)
	if retErr != nil {
		return nil, retErr
	}

	return keyspaceMetadata, nil
}

func (d *dseDB) DescribeKeyspaces(username string) ([]string, error) {
	rows, retErr := d.Select("SELECT keyspace_name FROM system_schema.keyspaces", username)
	if retErr != nil {
		return nil, retErr
	}

	var keyspaces []string

	for i := 0; i < len(rows); i++ {
		row := rows[i]["keyspace_name"]
		if str, ok := row.(string); ok {
			keyspaces = append(keyspaces, str)
		} else {
			log.With("column", rows[i]["keyspace_name"]).Warn("column is not a string")
		}
	}

	return keyspaces, nil
}

func (d *dseDB) DescribeTable(keyspace, table, username string) (*gocql.TableMetadata, error) {
	// Query system_schema first to make sure user is authorized
	stmt := "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?"

	rows, retErr := d.Select(stmt, username, keyspace, table)
	if retErr != nil {
		return nil, retErr
	}

	if len(rows) == 0 {
		return nil, e.NewNotFoundError(fmt.Sprintf("table %s in keyspace %s not found", table, keyspace))
	}

	keyspaceMetadata, retErr := d.session.KeyspaceMetadata(keyspace)
	if retErr != nil {
		return nil, retErr
	}

	for _, tableMetadata := range keyspaceMetadata.Tables {
		if tableMetadata.Name == table {
			return tableMetadata, nil
		}
	}

	return nil, e.NewNotFoundError(fmt.Sprintf("table %s in keyspace %s not found", table, keyspace))
}

func (d *dseDB) DescribeTables(keyspace, username string) ([]string, error) {
	// Query system_schema first to make sure user is authorized
	stmt := "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"
	rows, retErr := d.Select(stmt, username, keyspace)
	if retErr != nil {
		return nil, retErr
	}

	var tables []string
	for i := 0; i < len(rows); i++ {
		row := rows[i]["table_name"]
		if str, ok := row.(string); ok {
			tables = append(tables, str)
		} else {
			log.With("column", rows[i]["table_name"]).Warn("column is not a string")
		}
	}

	return tables, nil
}
