package config

import (
	"github.com/riptano/data-endpoints/log"
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
	o.On("ExcludedKeyspaces").Return(nil)
	o.On("SchemaUpdateInterval").Return(10 * time.Second)
	o.On("Naming").Return(NamingConventionFn(NewDefaultNaming))
	o.On("SupportedOperations").Return(Operations(0))
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

func (o *ConfigMock) SupportedOperations() Operations {
	args := o.Called()
	return args.Get(0).(Operations)
}

func (o *ConfigMock) UseUserOrRoleAuth() bool {
	args := o.Called()
	return args.Get(0).(bool)
}

func (o *ConfigMock) Logger() log.Logger {
	args := o.Called()
	return args.Get(0).(log.Logger)
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
