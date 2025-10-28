package stream

import (
	"context"
	"fmt"
	"strings"

	"github.com/delaneyj/witchbolt"
)

// BuildReplicas constructs replica implementations from configuration.
func BuildReplicas(ctx context.Context, cfg Config) ([]Replica, error) {
	var replicas []Replica
	for _, rc := range cfg.Replicas {
		var replica Replica
		var err error
		switch strings.ToLower(rc.Type) {
		case "file":
			replica, err = NewFileReplica(rc.Name, rc.File)
		case "s3":
			replica, err = NewS3CompatibleReplica(ctx, rc.Name, rc.S3)
		case "sftp":
			replica, err = NewSFTPReplica(ctx, rc.Name, rc.SFTP)
		case "nats":
			replica, err = NewNATSReplica(ctx, rc.Name, rc.NATS)
		default:
			err = fmt.Errorf("unknown replica type: %s", rc.Type)
		}
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
