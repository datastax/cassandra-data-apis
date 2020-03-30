package config

import (
	"github.com/stretchr/testify/mock"
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
	o.On("Naming").Return(DefaultNaming)
	o.On("SupportedOperations").Return(Operations(0))
	o.On("UseUserOrRoleAuth").Return(false)
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

func (o *ConfigMock) Naming() NamingConvention {
	args := o.Called()
	return args.Get(0).(NamingConvention)
}

func (o *ConfigMock) SupportedOperations() Operations {
	args := o.Called()
	return args.Get(0).(Operations)
}

func (o *ConfigMock) UseUserOrRoleAuth() bool {
	args := o.Called()
	return args.Get(0).(bool)
}