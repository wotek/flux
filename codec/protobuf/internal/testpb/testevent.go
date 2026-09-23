package testpb

import "github.com/wotek/flux"

var (
	_ flux.Event = (*TestProductCreated)(nil)
)

// Name returns the domain event name for TestProductCreated.
func (*TestProductCreated) Name() string {
	return "TestProductCreated"
}
