package virtualworkspace

import (
	"context"
	"strings"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
)

func newWorkspacedCluster(cfg *rest.Config, clusterName string, baseCluster cluster.Cluster) (*workspacedCluster, error) {
	cfg = rest.CopyConfig(cfg)
	cfg.Host = strings.TrimSuffix(cfg.Host, "/") + "/clusters/" + clusterName

	client, err := client.New(cfg, client.Options{
		Cache: &client.CacheOptions{
			Reader: baseCluster.GetAPIReader(),
		},
	})
	if err != nil {
		return nil, err
	}

	wsClient := &workspacedClient{
		Client:      client,
		clusterName: clusterName,
	}

	return &workspacedCluster{
		Cluster:     baseCluster,
		clusterName: clusterName,
		Client:      wsClient,
	}, nil
}

// workspacedCluster is a cluster that operates on a specific namespace.
type workspacedCluster struct {
	clusterName string
	cluster.Cluster
	cache cache.Cache
	client.Client
}

// Name returns the name of the cluster.
func (c *workspacedCluster) Name() string {
	return c.clusterName
}

// GetClient returns a client scoped to the namespace.
func (c *workspacedCluster) GetClient() client.Client {
	return c.Client
}

// GetEventRecorderFor returns a new EventRecorder for the provided name.
func (c *workspacedCluster) GetEventRecorderFor(name string) record.EventRecorder {
	panic("implement me")
}

// Start starts the cluster.
func (c *workspacedCluster) Start(ctx context.Context) error {
	return nil // no-op as this is shared
}
