package stream

import (
	"context"
	"fmt"

	"github.com/delaneyj/witchbolt"
)

// BuildReplicas constructs replica implementations from configuration.
func BuildReplicas(ctx context.Context, cfg Config) ([]Replica, error) {
	var replicas []Replica
	for i, rc := range cfg.Replicas {
		if rc == nil {
			return nil, fmt.Errorf("replica config at index %d is nil", i)
		}
		replica, err := rc.buildReplica(ctx)
		if err != nil {
			return nil, err
		}
		replicas = append(replicas, replica)
	}
	return replicas, nil
}

// Observer returns a PageFlushObserverRegistration that wires stream into witchbolt.Open options.
func Observer(ctx context.Context, cfg Config) witchbolt.PageFlushObserverRegistration {
	factoryCtx := ctx
	if factoryCtx == nil {
		factoryCtx = context.Background()
	}
	var ctrl *Controller
	return witchbolt.PageFlushObserverRegistration{
		Start: func(db *witchbolt.DB) (witchbolt.PageFlushObserver, error) {
			replicas, err := BuildReplicas(factoryCtx, cfg)
			if err != nil {
				return nil, err
			}
			ctrl, err = NewController(db, cfg, replicas)
			if err != nil {
				return nil, err
			}
			db.RegisterPageFlushObserver(ctrl)
			if err := ctrl.Start(factoryCtx); err != nil {
				db.UnregisterPageFlushObserver(ctrl)
				return nil, err
			}
			return ctrl, nil
		},
		Close: func() error {
			if ctrl == nil {
				return nil
			}
			return ctrl.Stop(factoryCtx)
		},
	}
}
