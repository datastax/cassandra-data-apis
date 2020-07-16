package config

import (
	"github.com/datastax/cassandra-data-apis/log"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"time"
)

type ConfigMock struct {
	mock.Mock
}

func NewConfigMock() *ConfigMock {
	return &ConfigMock{}
}

func (o *ConfigMock) Default() *ConfigMock {
	o.On("ExcludedKeyspaces").Return([]string{"system"})
	o.On("SchemaUpdateInterval").Return(10 * time.Second)
	o.On("Naming").Return(NamingConventionFn(NewDefaultNaming))
	o.On("UseUserOrRoleAuth").Return(false)
	o.On("Logger").Return(log.NewZapLogger(zap.NewExample()))
	return o
}

func (o *ConfigMock) ExcludedKeyspaces() []string {
	args := o.Called()
	return args.Get(0).([]string)
}

func (o *ConfigMock) SchemaUpdateInterval() time.Duration {
	args := o.Called()
	return args.Get(0).(time.Duration)
}

func (o *ConfigMock) Naming() NamingConventionFn {
	args := o.Called()
	return args.Get(0).(NamingConventionFn)
}

func (o *ConfigMock) UseUserOrRoleAuth() bool {
	args := o.Called()
	return args.Get(0).(bool)
}

func (o *ConfigMock) Logger() log.Logger {
	args := o.Called()
	return args.Get(0).(log.Logger)
}

func (o *ConfigMock) RouterInfo() HttpRouterInfo {
	args := o.Called()
	return args.Get(0).(HttpRouterInfo)
}

type KeyspaceNamingInfoMock struct {
	mock.Mock
}

func NewKeyspaceNamingInfoMock() *KeyspaceNamingInfoMock {
	return &KeyspaceNamingInfoMock{}
}

func (o *KeyspaceNamingInfoMock) Tables() map[string][]string {
	args := o.Called()
	return args.Get(0).(map[string][]string)
}
