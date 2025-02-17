package virtualworkspace

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/kontext"
)

const (
	// ClusterNameIndex indexes object by cluster and name.
	ClusterNameIndex = "cluster/name"
	// ClusterIndex indexes object by cluster.
	ClusterIndex = "cluster"

	clusterAnnotation = "kcp.io/cluster"
)

var _ cache.Cache = &workspacedCache{}

// workspacedCache is a cache that operates on a specific namespace.
type workspacedCache struct {
	clusterName string
	cache.Cache
	infGetter sharedInformerGetter
}

// Get returns a single object from the cache.
func (c *workspacedCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	inf, scope, found, err := c.infGetter(obj)
	if err != nil {
		return fmt.Errorf("failed to get informer for %T %s: %w", obj, obj.GetObjectKind().GroupVersionKind(), err)
	}
	if !found {
		return fmt.Errorf("no informer found for %T %s", obj, obj.GetObjectKind().GroupVersionKind())
	}
	c.Cache.List(ctx, obj, client.MatchingFields{})

	if c.scopeName == apimeta.RESTScopeNameRoot {
		key.Namespace = ""
	}
	storeKey := objectKeyToStoreKey(key)

	// create cluster-aware key for KCP
	_, isClusterAware := c.indexer.GetIndexers()[kcpcache.ClusterAndNamespaceIndexName]
	clusterName, _ := kontext.ClusterFrom(ctx)
	if isClusterAware && clusterName.Empty() {
		return fmt.Errorf("cluster-aware cache requires a cluster in context")
	}
	if isClusterAware {
		storeKey = clusterName.String() + "|" + storeKey
	}

	// Lookup the object from the indexer cache
	obj, exists, err := c.indexer.GetByKey(storeKey)
	if err != nil {
		return err
	}

	// Not found, return an error
	if !exists {
		return apierrors.NewNotFound(schema.GroupResource{
			Group: c.groupVersionKind.Group,
			// Resource gets set as Kind in the error so this is fine
			Resource: c.groupVersionKind.Kind,
		}, key.Name)
	}

	// Verify the result is a runtime.Object
	if _, isObj := obj.(runtime.Object); !isObj {
		// This should never happen
		return fmt.Errorf("cache contained %T, which is not an Object", obj)
	}

	if c.disableDeepCopy {
		// skip deep copy which might be unsafe
		// you must DeepCopy any object before mutating it outside
	} else {
		// deep copy to avoid mutating cache
		obj = obj.(runtime.Object).DeepCopyObject()
	}

	// Copy the value of the item in the cache to the returned value
	// TODO(directxman12): this is a terrible hack, pls fix (we should have deepcopyinto)
	outVal := reflect.ValueOf(out)
	objVal := reflect.ValueOf(obj)
	if !objVal.Type().AssignableTo(outVal.Type()) {
		return fmt.Errorf("cache had type %s, but %s was asked for", objVal.Type(), outVal.Type())
	}
	reflect.Indirect(outVal).Set(reflect.Indirect(objVal))
	if !c.disableDeepCopy {
		out.GetObjectKind().SetGroupVersionKind(c.groupVersionKind)
	}

	return nil
}

// List returns a list of objects from the cache.
func (c *workspacedCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	var listOpts client.ListOptions
	for _, o := range opts {
		o.ApplyToList(&listOpts)
	}

	if err := c.Cache.List(ctx, list, opts...); err != nil {
		return err
	}

	return nil
}

// GetInformer returns an informer for the given object kind.
func (c *workspacedCache) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	inf, err := c.Cache.GetInformer(ctx, obj, opts...)
	if err != nil {
		return nil, err
	}
	return &scopedInformer{clusterName: c.clusterName, Informer: inf}, nil
}

// GetInformerForKind returns an informer for the given GroupVersionKind.
func (c *workspacedCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, opts ...cache.InformerGetOption) (cache.Informer, error) {
	inf, err := c.Cache.GetInformerForKind(ctx, gvk, opts...)
	if err != nil {
		return nil, err
	}
	return &scopedInformer{clusterName: c.clusterName, Informer: inf}, nil
}

// RemoveInformer removes an informer from the cache.
func (c *workspacedCache) RemoveInformer(ctx context.Context, obj client.Object) error {
	return errors.New("informer cannot be removed from scoped cache")
}

// scopedInformer is an informer that operates on a specific namespace.
type scopedInformer struct {
	clusterName string
	cache.Informer
}

// AddEventHandler adds an event handler to the informer.
func (i *scopedInformer) AddEventHandler(handler toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	return i.Informer.AddEventHandler(toolscache.ResourceEventHandlerDetailedFuncs{
		AddFunc: func(obj interface{}, isInInitialList bool) {
			cobj := obj.(client.Object)
			if cobj.GetAnnotations()[clusterAnnotation] == i.clusterName {
				cobj := cobj.DeepCopyObject().(client.Object)
				handler.OnAdd(cobj, isInInitialList)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			cobj := newObj.(client.Object)
			cold := oldObj.(client.Object)
			if cobj.GetAnnotations()[clusterAnnotation] == i.clusterName {
				cobj := cobj.DeepCopyObject().(client.Object)
				cold := cold.DeepCopyObject().(client.Object)
				handler.OnUpdate(cold, cobj)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if tombStone, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
				obj = tombStone.Obj
			}
			cobj := obj.(client.Object)
			if cobj.GetAnnotations()[clusterAnnotation] == i.clusterName {
				cobj := cobj.DeepCopyObject().(client.Object)
				handler.OnDelete(cobj)
			}
		},
	})
}

// AddEventHandlerWithResyncPeriod adds an event handler to the informer with a resync period.
func (i *scopedInformer) AddEventHandlerWithResyncPeriod(handler toolscache.ResourceEventHandler, resyncPeriod time.Duration) (toolscache.ResourceEventHandlerRegistration, error) {
	return i.Informer.AddEventHandlerWithResyncPeriod(toolscache.ResourceEventHandlerDetailedFuncs{
		AddFunc: func(obj interface{}, isInInitialList bool) {
			cobj := obj.(client.Object)
			if cobj.GetAnnotations()[clusterAnnotation] == i.clusterName {
				cobj := cobj.DeepCopyObject().(client.Object)
				handler.OnAdd(cobj, isInInitialList)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			obj := newObj.(client.Object)
			if obj.GetAnnotations()[clusterAnnotation] == i.clusterName {
				obj := obj.DeepCopyObject().(client.Object)
				old := oldObj.(client.Object).DeepCopyObject().(client.Object)
				handler.OnUpdate(old, obj)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if tombStone, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
				obj = tombStone.Obj
			}
			cobj := obj.(client.Object)
			if cobj.GetAnnotations()[clusterAnnotation] == i.clusterName {
				cobj := cobj.DeepCopyObject().(client.Object)
				handler.OnDelete(cobj)
			}
		},
	}, resyncPeriod)
}

// AddIndexers adds indexers to the informer.
func (i *scopedInformer) AddIndexers(indexers toolscache.Indexers) error {
	return errors.New("indexes cannot be added to scoped informers")
}

// workspaceScopeableCache is a cache that indexes objects by namespace.
type workspaceScopeableCache struct { //nolint:revive // Stuttering here is fine.
	cache.Cache
}

// IndexField adds an index for the given object kind.
func (f *workspaceScopeableCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	return f.Cache.IndexField(ctx, obj, "cluster/"+field, func(obj client.Object) []string {
		keys := extractValue(obj)
		withCluster := make([]string, len(keys)*2)
		for i, key := range keys {
			withCluster[i] = fmt.Sprintf("%s/%s", obj.GetAnnotations()[clusterAnnotation], key)
			withCluster[i+len(keys)] = fmt.Sprintf("*/%s", key)
		}
		return withCluster
	})
}

// Start starts the cache.
func (f *workspaceScopeableCache) Start(ctx context.Context) error {
	return nil
}
