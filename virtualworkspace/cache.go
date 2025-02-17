package virtualworkspace

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

	disableDeepCopy bool
}

/*
// IndexField adds an indexer to the underlying informer, using extractValue function to get
// value(s) from the given field. This index can then be used by passing a field selector
// to List. For one-to-one compatibility with "normal" field selectors, only return one value.
// The values may be anything. They will automatically be prefixed with the namespace of the
// given object, if present. The objects passed are guaranteed to be objects of the correct type.
func (ic *workspacedCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	informer, err := ic.GetInformer(ctx, obj)
	if err != nil {
		return err
	}
	return ic.indexByField(informer, field, extractValue)
}

func (ic *workspacedCache) indexByField(informer cache.Informer, field string, extractValue client.IndexerFunc) error {
	indexFunc := func(objRaw interface{}) ([]string, error) {
		// TODO(directxman12): check if this is the correct type?
		obj, isObj := objRaw.(client.Object)
		if !isObj {
			return nil, fmt.Errorf("object of type %T is not an Object", objRaw)
		}
		meta, err := apimeta.Accessor(obj)
		if err != nil {
			return nil, err
		}
		ns := meta.GetNamespace()

		keyFunc := func(ns, val string) string {
			return KeyToClusteredKey(ic.clusterName, ns, val)
		}

		rawVals := extractValue(obj)
		var vals []string
		if ns == "" {
			// if we're not doubling the keys for the namespaced case, just create a new slice with same length
			vals = make([]string, len(rawVals))
		} else {
			// if we need to add non-namespaced versions too, double the length
			vals = make([]string, len(rawVals)*2)
		}

		for i, rawVal := range rawVals {
			// save a namespaced variant, so that we can ask
			// "what are all the object matching a given index *in a given namespace*"
			vals[i] = keyFunc(ns, rawVal)
			if ns != "" {
				// if we have a namespace, also inject a special index key for listing
				// regardless of the object namespace
				vals[i+len(rawVals)] = keyFunc("", rawVal)
			}
		}

		return vals, nil
	}

	return informer.AddIndexers(toolscache.Indexers{FieldIndexName(field): indexFunc})
}

// KeyToClusteredKey prefixes the given index key with a cluster name
// for use in field selector indexes.
func KeyToClusteredKey(clusterName string, ns string, baseKey string) string {
	return clusterName + "|" + KeyToNamespacedKey(ns, baseKey)
}

// KeyToNamespacedKey prefixes the given index key with a namespace
// for use in field selector indexes.
func KeyToNamespacedKey(ns string, baseKey string) string {
	if ns != "" {
		return ns + "/" + baseKey
	}
	return allNamespacesNamespace + "/" + baseKey
}

// allNamespacesNamespace is used as the "namespace" when we want to list across all namespaces.
const allNamespacesNamespace = "__all_namespaces"

// FieldIndexName constructs the name of the index over the given field,
// for use with an indexer.
func FieldIndexName(field string) string {
	return "field:" + field
}
*/
/*
func (c *workspacedCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.Cache.Get(ctx, key, obj, opts...)
}
*/

// Get returns a single object from the cache.
func (c *workspacedCache) Get(ctx context.Context, key client.ObjectKey, out client.Object, opts ...client.GetOption) error {
	inf, _, gvk, found, err := c.infGetter(out)
	if err != nil {
		return fmt.Errorf("failed to get informer for %T %s: %w", out, out.GetObjectKind().GroupVersionKind(), err)
	}
	if !found {
		return fmt.Errorf("no informer found for %T %s", out, out.GetObjectKind().GroupVersionKind())
	}

	storeKey := objectKeyToStoreKey(key)

	// create cluster-aware key for KCP
	/*
		_, isClusterAware := inf.GetIndexer().GetIndexers()[ClusterNameIndex]
		if isClusterAware && c.clusterName == "" {
			return fmt.Errorf("cluster-aware cache requires a cluster")
		}
		if isClusterAware {
			storeKey = c.clusterName + "/" + storeKey
		}
	*/

	// Lookup the object from the indexer cache
	o, exists, err := inf.GetIndexer().GetByKey(storeKey)
	if err != nil {
		return err
	}

	// Not found, return an error
	if !exists {
		return apierrors.NewNotFound(schema.GroupResource{Group: gvk.Group}, key.Name)
	}

	// Verify the result is a runtime.Object
	obj, isObj := o.(runtime.Object)
	if !isObj {
		// This should never happen
		return fmt.Errorf("cache contained %T, which is not an Object", obj)
	}

	if c.disableDeepCopy {
		// skip deep copy which might be unsafe
		// you must DeepCopy any object before mutating it outside
	} else {
		// deep copy to avoid mutating cache
		obj = obj.DeepCopyObject()
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
		out.GetObjectKind().SetGroupVersionKind(gvk)
	}

	return nil
}

func objectKeyToStoreKey(k client.ObjectKey) string {
	if k.Namespace == "" {
		return k.Name
	}
	return k.Namespace + "/" + k.Name
}

// List returns a list of objects from the cache.
func (c *workspacedCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return c.Cache.List(ctx, list, opts...)
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
		}
		return withCluster
	})
}

// Start starts the cache.
func (f *workspaceScopeableCache) Start(ctx context.Context) error {
	return f.Cache.Start(ctx)
}
