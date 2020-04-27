package db

import (
	"github.com/gocql/gocql"
	"go.uber.org/atomic"
)

type dcInferringPolicy struct {
	childPolicy gocql.HostSelectionPolicy
	localDc     atomic.String
}

func NewDcInferringPolicy() gocql.HostSelectionPolicy {
	return gocql.TokenAwareHostPolicy(&dcInferringPolicy{
		childPolicy: gocql.DCAwareRoundRobinPolicy("__invalid_dc_not_considered"),
	})
}

func (p *dcInferringPolicy) AddHost(host *gocql.HostInfo) {
	if p.localDc.Load() == "" {
		p.localDc.Store(host.DataCenter())
	}
	p.childPolicy.AddHost(host)
}

func (p *dcInferringPolicy) RemoveHost(host *gocql.HostInfo) {
	p.childPolicy.RemoveHost(host)
}

func (p *dcInferringPolicy) HostUp(host *gocql.HostInfo) {
	p.childPolicy.HostUp(host)
}

func (p *dcInferringPolicy) HostDown(host *gocql.HostInfo) {
	p.childPolicy.HostDown(host)
}

func (p *dcInferringPolicy) SetPartitioner(partitioner string) {
	p.childPolicy.SetPartitioner(partitioner)
}

func (p *dcInferringPolicy) KeyspaceChanged(gocql.KeyspaceUpdateEvent) {}

func (p *dcInferringPolicy) Init(*gocql.Session) {}

func (p *dcInferringPolicy) IsLocal(host *gocql.HostInfo) bool {
	localDc := p.localDc.Load()
	if localDc == "" {
		return true
	}

	return host.DataCenter() == localDc
}

func (p *dcInferringPolicy) Pick(query gocql.ExecutableQuery) gocql.NextHost {
	return p.childPolicy.Pick(query)
}
