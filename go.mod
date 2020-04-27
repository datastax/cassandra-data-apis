module github.com/datastax/cassandra-data-apis

go 1.13

require (
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/gocql/gocql v0.0.0-20200228163523-cd4b606dd2fb
	github.com/graphql-go/graphql v0.7.9
	github.com/iancoleman/strcase v0.0.0-20191112232945-16388991a334
	github.com/julienschmidt/httprouter v1.3.0
	github.com/mitchellh/mapstructure v1.2.2
	github.com/onsi/ginkgo v1.12.0
	github.com/onsi/gomega v1.9.0
	github.com/pelletier/go-toml v1.7.0 // indirect
	github.com/spf13/afero v1.2.2 // indirect
	github.com/spf13/cast v1.3.1 // indirect
	github.com/spf13/cobra v0.0.7
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.6.2
	github.com/stretchr/objx v0.2.0 // indirect
	github.com/stretchr/testify v1.5.1
	go.uber.org/atomic v1.6.0
	go.uber.org/zap v1.14.1
	golang.org/x/sys v0.0.0-20200331124033-c3d80250170d // indirect
	golang.org/x/text v0.3.2 // indirect
	gopkg.in/inf.v0 v0.9.1
	gopkg.in/ini.v1 v1.55.0 // indirect
	gopkg.in/yaml.v2 v2.2.8 // indirect
)

replace github.com/graphql-go/graphql => github.com/riptano/graphql-go v0.7.9-null
