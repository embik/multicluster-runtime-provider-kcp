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

/*
// WithClusterNameIndex adds indexers for cluster name and namespace.
func WithClusterNameIndex() cluster.Option {
	return func(options *cluster.Options) {
		old := options.Cache.NewInformer
		options.Cache.NewInformer = func(watcher toolscache.ListerWatcher, object apiruntime.Object, duration time.Duration, indexers toolscache.Indexers) toolscache.SharedIndexInformer {
			var inf toolscache.SharedIndexInformer
			if old != nil {
				inf = old(watcher, object, duration, indexers)
			} else {
				inf = toolscache.NewSharedIndexInformer(watcher, object, duration, indexers)
			}
			if err := inf.AddIndexers(toolscache.Indexers{
				ClusterNameIndex: func(obj any) ([]string, error) {
					o := obj.(client.Object)
					return []string{
						fmt.Sprintf("%s/%s", o.GetAnnotations()[clusterAnnotation], o.GetName()),
						fmt.Sprintf("%s/%s", "*", o.GetName()),
					}, nil
				},
				ClusterIndex: func(obj any) ([]string, error) {
					o := obj.(client.Object)
					return []string{o.GetAnnotations()[clusterAnnotation]}, nil
				},
			}); err != nil {
				utilruntime.HandleError(fmt.Errorf("unable to add cluster name indexers: %w", err))
			}
			return inf
		}
	}
}
*/

func newWorkspacedCluster(cfg *rest.Config, clusterName string, baseCluster cluster.Cluster, infGetter sharedInformerGetter) (*workspacedCluster, error) {
	cfg = rest.CopyConfig(cfg)
	cfg.Host = strings.TrimSuffix(cfg.Host, "/") + "/clusters/" + clusterName

	c := &workspacedCache{
		clusterName: clusterName,
		Cache:       baseCluster.GetCache(),
		infGetter:   infGetter,
	}

	client, err := client.New(cfg, client.Options{
		Cache: &client.CacheOptions{
			Reader: c,
		},
	})
	if err != nil {
		return nil, err
	}

	return &workspacedCluster{
		Cluster:     baseCluster,
		clusterName: clusterName,
		Client:      client,
		cache:       c,
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

// GetCache returns a cache.Cache.
func (c *workspacedCluster) GetCache() cache.Cache {
	return c.cache
}

// GetClient returns a client scoped to the namespace.
func (c *workspacedCluster) GetClient() client.Client {
	return c.Client
}

// GetEventRecorderFor returns a new EventRecorder for the provided name.
func (c *workspacedCluster) GetEventRecorderFor(name string) record.EventRecorder {
	panic("implement me")
}

// GetAPIReader returns a reader against the cluster.
func (c *workspacedCluster) GetAPIReader() client.Reader {
	return c.GetAPIReader()
}

// Start starts the cluster.
func (c *workspacedCluster) Start(ctx context.Context) error {
	return nil // no-op as this is shared
}
