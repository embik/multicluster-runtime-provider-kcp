package virtualworkspace

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/multicluster-runtime/multicluster-runtime/pkg/multicluster"

	mcmanager "github.com/multicluster-runtime/multicluster-runtime/pkg/manager"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var _ multicluster.Provider = &Provider{}

// Provider is a cluster provider that represents each namespace
// as a dedicated cluster with only a "default" namespace. It maps each namespace
// to "default" and vice versa, simulating a multi-cluster setup. It uses one
// informer to watch objects for all namespaces.
type Provider struct {
	config *rest.Config

	cluster   cluster.Cluster
	infGetter sharedInformerGetter

	log logr.Logger

	lock      sync.RWMutex
	clusters  map[string]cluster.Cluster
	cancelFns map[string]context.CancelFunc
}

// New creates a new namespace provider.
func New(restConfig *rest.Config) (*Provider, error) {
	config := rest.CopyConfig(restConfig)
	if !strings.HasSuffix(config.Host, "/clusters/*") {
		config.Host += "/clusters/*"
	}

	var infGetter sharedInformerGetter
	withGlobalSettings := func(opts *cluster.Options) {
		opts.NewCache = func(config *rest.Config, opts cache.Options) (cache.Cache, error) {
			opts, infGetter = withClusterNameIndex(opts)
			c, err := cache.New(config, opts)
			if err != nil {
				return nil, err
			}
			return &workspaceScopeableCache{Cache: c}, nil
		}
	}

	wildcardCluster, err := cluster.New(config, withGlobalSettings)
	if err != nil {
		return nil, fmt.Errorf("failed to construct wildcard cluster: %w", err)
	}

	return &Provider{
		config:    restConfig,
		cluster:   wildcardCluster,
		infGetter: infGetter,

		log: log.Log.WithName("kcp-virtualworkspace-cluster-provider"),

		clusters:  map[string]cluster.Cluster{},
		cancelFns: map[string]context.CancelFunc{},
	}, nil
}

// Run starts the provider and blocks.
func (p *Provider) Run(ctx context.Context, mgr mcmanager.Manager) error {
	go func() {
		if err := p.cluster.Start(ctx); err != nil {
			p.log.Error(err, "failed to start wildcard cache")
		}
	}()
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

	cl, err := newWorkspacedCluster(p.config, clusterName, p.cluster, p.infGetter)
	if err != nil {
		return nil, err
	}
	p.clusters[clusterName] = cl

	return cl, nil
}
