package virtualworkspace

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/multicluster-runtime/multicluster-runtime/pkg/multicluster"

	kcpcache "github.com/kcp-dev/apimachinery/v2/pkg/cache"
	"github.com/kcp-dev/apimachinery/v2/third_party/informers"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	k8scache "k8s.io/client-go/tools/cache"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/kcp-dev/multicluster-runtime-provider/internal/cache"

	mcmanager "github.com/multicluster-runtime/multicluster-runtime/pkg/manager"
)

var _ multicluster.Provider = &Provider{}

// Provider is a cluster provider that represents each namespace
// as a dedicated cluster with only a "default" namespace. It maps each namespace
// to "default" and vice versa, simulating a multi-cluster setup. It uses one
// informer to watch objects for all namespaces.
type Provider struct {
	config *rest.Config

	cluster cluster.Cluster

	log logr.Logger

	lock      sync.RWMutex
	clusters  map[string]cluster.Cluster
	cancelFns map[string]context.CancelFunc
}

// NewClusterAwareCache returns a cache.Cache that handles multi-cluster watches.
func NewClusterAwareCache(config *rest.Config, opts ctrlcache.Options) (ctrlcache.Cache, error) {
	c := rest.CopyConfig(config)

	opts.NewInformer = NewInformerWithClusterIndexes
	return cache.New(c, opts)
}

// New creates a new namespace provider.
func New(restConfig *rest.Config) (*Provider, error) {
	config := rest.CopyConfig(restConfig)
	if !strings.HasSuffix(config.Host, "/clusters/*") {
		config.Host += "/clusters/*"
	}

	withGlobalSettings := func(opts *cluster.Options) {
		opts.NewCache = NewClusterAwareCache
	}

	wildcardCluster, err := cluster.New(config, withGlobalSettings)
	if err != nil {
		return nil, fmt.Errorf("failed to construct wildcard cluster: %w", err)
	}

	return &Provider{
		config:  restConfig,
		cluster: wildcardCluster,

		log: log.Log.WithName("kcp-virtualworkspace-cluster-provider"),

		clusters:  map[string]cluster.Cluster{},
		cancelFns: map[string]context.CancelFunc{},
	}, nil
}

// Run starts the provider and blocks.
func (p *Provider) Run(ctx context.Context, mgr mcmanager.Manager) error {
	go func() {
		p.log.Info("Starting wildcard cluster")
		if err := p.cluster.Start(ctx); err != nil {
			p.log.Error(err, "failed to start wildcard cluster")
		}
	}()

	syncCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if !p.cluster.GetCache().WaitForCacheSync(syncCtx) {
		return fmt.Errorf("failed to sync wildcard cache")
	}

	p.log.Info("Wildcard cache finished sync")

	if mgr != nil {
		p.lock.Lock()
		defer p.lock.Unlock()

		mgr.Engage(ctx, "*", p.cluster)
	}

	return nil
}

// Get returns a cluster by name.
func (p *Provider) Get(ctx context.Context, clusterName string) (cluster.Cluster, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if cl, ok := p.clusters[clusterName]; ok {
		return cl, nil
	}

	cl, err := newWorkspacedCluster(p.config, clusterName, p.cluster)
	if err != nil {
		return nil, err
	}
	p.clusters[clusterName] = cl

	return cl, nil
}

// NewInformerWithClusterIndexes returns a SharedIndexInformer that is configured
// ClusterIndexName and ClusterAndNamespaceIndexName indexes.
func NewInformerWithClusterIndexes(lw k8scache.ListerWatcher, obj runtime.Object, syncPeriod time.Duration, indexers k8scache.Indexers) k8scache.SharedIndexInformer {
	indexers[kcpcache.ClusterIndexName] = kcpcache.ClusterIndexFunc
	indexers[kcpcache.ClusterAndNamespaceIndexName] = kcpcache.ClusterAndNamespaceIndexFunc

	return informers.NewSharedIndexInformer(lw, obj, syncPeriod, indexers)
}
