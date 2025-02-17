package virtualworkspace

import (
	"fmt"
	"sync"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

type sharedInformerGetter func(obj apiruntime.Object) (toolscache.SharedIndexInformer, apimeta.RESTScopeName, schema.GroupVersionKind, bool, error)

func withClusterNameIndex(opts cache.Options) (cache.Options, sharedInformerGetter) {
	old := opts.NewInformer

	tracker := informerTracker{
		Structured:   make(map[schema.GroupVersionKind]*toolscache.SharedIndexInformer),
		Unstructured: make(map[schema.GroupVersionKind]*toolscache.SharedIndexInformer),
		Metadata:     make(map[schema.GroupVersionKind]*toolscache.SharedIndexInformer),
	}

	opts.NewInformer = func(watcher toolscache.ListerWatcher, obj apiruntime.Object, duration time.Duration, indexers toolscache.Indexers) toolscache.SharedIndexInformer {
		gvk, err := apiutil.GVKForObject(obj, opts.Scheme)
		if err != nil {
			panic(err)
		}

		var inf toolscache.SharedIndexInformer
		if old != nil {
			inf = old(watcher, obj, duration, indexers)
		} else {
			inf = toolscache.NewSharedIndexInformer(watcher, obj, duration, indexers)
		}
		if err := inf.AddIndexers(toolscache.Indexers{
			ClusterNameIndex: func(obj any) ([]string, error) {
				o := obj.(client.Object)
				return []string{
					fmt.Sprintf("%s/%s", o.GetAnnotations()[clusterAnnotation], o.GetName()),
				}, nil
			},
			ClusterIndex: func(obj any) ([]string, error) {
				o := obj.(client.Object)
				return []string{o.GetAnnotations()[clusterAnnotation]}, nil
			},
		}); err != nil {
			utilruntime.HandleError(fmt.Errorf("unable to add cluster name indexers: %w", err))
		}

		infs := tracker.informersByType(obj)
		tracker.lock.Lock()
		if _, ok := infs[gvk]; ok {
			panic(fmt.Sprintf("informer for %s already exists", gvk))
		}
		infs[gvk] = &inf
		tracker.lock.Unlock()

		return inf
	}

	return opts, func(obj apiruntime.Object) (toolscache.SharedIndexInformer, apimeta.RESTScopeName, schema.GroupVersionKind, bool, error) {
		gvk, err := apiutil.GVKForObject(obj, opts.Scheme)
		if err != nil {
			return nil, "", schema.GroupVersionKind{}, false, err
		}

		mapping, err := opts.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return nil, "", schema.GroupVersionKind{}, false, err
		}

		infs := tracker.informersByType(obj)
		tracker.lock.RLock()
		inf, ok := infs[gvk]
		tracker.lock.RUnlock()
		if !ok || inf == nil {
			return nil, "", schema.GroupVersionKind{}, false, fmt.Errorf("informer not found")
		}

		return *inf, mapping.Scope.Name(), gvk, ok, nil
	}
}

type informerTracker struct {
	lock         sync.RWMutex
	Structured   map[schema.GroupVersionKind]*toolscache.SharedIndexInformer
	Unstructured map[schema.GroupVersionKind]*toolscache.SharedIndexInformer
	Metadata     map[schema.GroupVersionKind]*toolscache.SharedIndexInformer
}

func (t *informerTracker) informersByType(obj apiruntime.Object) map[schema.GroupVersionKind]*toolscache.SharedIndexInformer {
	switch obj.(type) {
	case apiruntime.Unstructured:
		return t.Unstructured
	case *metav1.PartialObjectMetadata, *metav1.PartialObjectMetadataList:
		return t.Metadata
	default:
		return t.Structured
	}
}
