package db

import (
	"github.com/gocql/gocql"
	"sync/atomic"
)

type dcInferringPolicy struct {
	childPolicy  atomic.Value
	isLocalDcSet int32
}

type childPolicyWrapper struct {
	policy gocql.HostSelectionPolicy
}

func NewDefaultHostSelectionPolicy() gocql.HostSelectionPolicy {
	return gocql.TokenAwareHostPolicy(NewDcInferringPolicy(), gocql.ShuffleReplicas())
}

func NewDcInferringPolicy() *dcInferringPolicy {
	policy := dcInferringPolicy{}
	policy.childPolicy.Store(childPolicyWrapper{gocql.RoundRobinHostPolicy()})
	return &policy
}

func (p *dcInferringPolicy) AddHost(host *gocql.HostInfo) {
	if atomic.CompareAndSwapInt32(&p.isLocalDcSet, 0, 1) {
		childPolicy := gocql.DCAwareRoundRobinPolicy(host.DataCenter())
		p.childPolicy.Store(childPolicyWrapper{childPolicy})
		childPolicy.AddHost(host)
	} else {
		p.getChildPolicy().AddHost(host)
	}
}

func (p *dcInferringPolicy) getChildPolicy() gocql.HostSelectionPolicy {
	wrapper := p.childPolicy.Load().(childPolicyWrapper)
	return wrapper.policy
}

func (p *dcInferringPolicy) RemoveHost(host *gocql.HostInfo) {
	p.getChildPolicy().RemoveHost(host)
}

func (p *dcInferringPolicy) HostUp(host *gocql.HostInfo) {
	p.getChildPolicy().HostUp(host)
}

func (p *dcInferringPolicy) HostDown(host *gocql.HostInfo) {
	p.getChildPolicy().HostDown(host)
}

func (p *dcInferringPolicy) SetPartitioner(partitioner string) {
	p.getChildPolicy().SetPartitioner(partitioner)
}

func (p *dcInferringPolicy) KeyspaceChanged(e gocql.KeyspaceUpdateEvent) {
	p.getChildPolicy().KeyspaceChanged(e)
}

func (p *dcInferringPolicy) Init(*gocql.Session) {
	// TAP parent policy does not call init on "fallback policy"
}

func (p *dcInferringPolicy) IsLocal(host *gocql.HostInfo) bool {
	return p.getChildPolicy().IsLocal(host)
}

func (p *dcInferringPolicy) Pick(query gocql.ExecutableQuery) gocql.NextHost {
	return p.getChildPolicy().Pick(query)
}
