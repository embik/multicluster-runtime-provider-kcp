/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"golang.org/x/exp/maps"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"

	"github.com/kcp-dev/multicluster-runtime-provider/internal/cache/internal"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

var (
	defaultSyncPeriod = 10 * time.Hour
)

// New initializes and returns a new Cache.
func New(cfg *rest.Config, opts cache.Options) (cache.Cache, error) {
	opts, err := defaultOpts(cfg, opts)
	if err != nil {
		return nil, err
	}

	newCacheFunc := newCache(cfg, opts)

	var defaultCache cache.Cache
	if len(opts.DefaultNamespaces) > 0 {
		defaultConfig := optionDefaultsToConfig(&opts)
		defaultCache = newMultiNamespaceCache(newCacheFunc, opts.Scheme, opts.Mapper, opts.DefaultNamespaces, &defaultConfig)
	} else {
		defaultCache = newCacheFunc(optionDefaultsToConfig(&opts), corev1.NamespaceAll)
	}

	if len(opts.ByObject) == 0 {
		return defaultCache, nil
	}

	delegating := &delegatingByGVKCache{
		scheme:       opts.Scheme,
		caches:       make(map[schema.GroupVersionKind]cache.Cache, len(opts.ByObject)),
		defaultCache: defaultCache,
	}

	for obj, config := range opts.ByObject {
		gvk, err := apiutil.GVKForObject(obj, opts.Scheme)
		if err != nil {
			return nil, fmt.Errorf("failed to get GVK for type %T: %w", obj, err)
		}
		var cache cache.Cache
		if len(config.Namespaces) > 0 {
			cache = newMultiNamespaceCache(newCacheFunc, opts.Scheme, opts.Mapper, config.Namespaces, nil)
		} else {
			cache = newCacheFunc(byObjectToConfig(config), corev1.NamespaceAll)
		}
		delegating.caches[gvk] = cache
	}

	return delegating, nil
}

func optionDefaultsToConfig(opts *cache.Options) cache.Config {
	return cache.Config{
		LabelSelector:         opts.DefaultLabelSelector,
		FieldSelector:         opts.DefaultFieldSelector,
		Transform:             opts.DefaultTransform,
		UnsafeDisableDeepCopy: opts.DefaultUnsafeDisableDeepCopy,
	}
}

func byObjectToConfig(byObject cache.ByObject) cache.Config {
	return cache.Config{
		LabelSelector:         byObject.Label,
		FieldSelector:         byObject.Field,
		Transform:             byObject.Transform,
		UnsafeDisableDeepCopy: byObject.UnsafeDisableDeepCopy,
	}
}

type newCacheFunc func(config cache.Config, namespace string) cache.Cache

func newCache(restConfig *rest.Config, opts cache.Options) newCacheFunc {
	return func(config cache.Config, namespace string) cache.Cache {
		return &informerCache{
			scheme: opts.Scheme,
			Informers: internal.NewInformers(restConfig, &internal.InformersOpts{
				HTTPClient:   opts.HTTPClient,
				Scheme:       opts.Scheme,
				Mapper:       opts.Mapper,
				ResyncPeriod: *opts.SyncPeriod,
				Namespace:    namespace,
				Selector: internal.Selector{
					Label: config.LabelSelector,
					Field: config.FieldSelector,
				},
				Transform:             config.Transform,
				WatchErrorHandler:     opts.DefaultWatchErrorHandler,
				UnsafeDisableDeepCopy: ptr.Deref(config.UnsafeDisableDeepCopy, false),
				NewInformer:           opts.NewInformer,
			}),
			readerFailOnMissingInformer: opts.ReaderFailOnMissingInformer,
			clusterIndexes:              strings.HasSuffix(restConfig.Host, "/clusters/*"),
		}
	}
}

func defaultOpts(config *rest.Config, opts cache.Options) (cache.Options, error) {
	config = rest.CopyConfig(config)
	if config.UserAgent == "" {
		config.UserAgent = rest.DefaultKubernetesUserAgent()
	}

	// Use the rest HTTP client for the provided config if unset
	if opts.HTTPClient == nil {
		var err error
		opts.HTTPClient, err = rest.HTTPClientFor(config)
		if err != nil {
			return cache.Options{}, fmt.Errorf("could not create HTTP client from config: %w", err)
		}
	}

	// Use the default Kubernetes Scheme if unset
	if opts.Scheme == nil {
		opts.Scheme = scheme.Scheme
	}

	// Construct a new Mapper if unset
	if opts.Mapper == nil {
		var err error
		opts.Mapper, err = apiutil.NewDynamicRESTMapper(config, opts.HTTPClient)
		if err != nil {
			return cache.Options{}, fmt.Errorf("could not create RESTMapper from config: %w", err)
		}
	}

	for obj, byObject := range opts.ByObject {
		isNamespaced, err := apiutil.IsObjectNamespaced(obj, opts.Scheme, opts.Mapper)
		if err != nil {
			return opts, fmt.Errorf("failed to determine if %T is namespaced: %w", obj, err)
		}
		if !isNamespaced && byObject.Namespaces != nil {
			return opts, fmt.Errorf("type %T is not namespaced, but its ByObject.Namespaces setting is not nil", obj)
		}

		if isNamespaced && byObject.Namespaces == nil {
			byObject.Namespaces = maps.Clone(opts.DefaultNamespaces)
		}

		// Default the namespace-level configs first, because they need to use the undefaulted type-level config
		// to be able to potentially fall through to settings from DefaultNamespaces.
		for namespace, config := range byObject.Namespaces {
			// 1. Default from the undefaulted type-level config
			config = defaultConfig(config, byObjectToConfig(byObject))

			// 2. Default from the namespace-level config. This was defaulted from the global default config earlier, but
			//    might not have an entry for the current namespace.
			if defaultNamespaceSettings, hasDefaultNamespace := opts.DefaultNamespaces[namespace]; hasDefaultNamespace {
				config = defaultConfig(config, defaultNamespaceSettings)
			}

			// 3. Default from the global defaults
			config = defaultConfig(config, optionDefaultsToConfig(&opts))

			if namespace == metav1.NamespaceAll {
				config.FieldSelector = fields.AndSelectors(
					appendIfNotNil(
						namespaceAllSelector(maps.Keys(byObject.Namespaces)),
						config.FieldSelector,
					)...,
				)
			}

			byObject.Namespaces[namespace] = config
		}

		// Only default ByObject iself if it isn't namespaced or has no namespaces configured, as only
		// then any of this will be honored.
		if !isNamespaced || len(byObject.Namespaces) == 0 {
			defaultedConfig := defaultConfig(byObjectToConfig(byObject), optionDefaultsToConfig(&opts))
			byObject.Label = defaultedConfig.LabelSelector
			byObject.Field = defaultedConfig.FieldSelector
			byObject.Transform = defaultedConfig.Transform
			byObject.UnsafeDisableDeepCopy = defaultedConfig.UnsafeDisableDeepCopy
		}

		opts.ByObject[obj] = byObject
	}

	// Default namespaces after byObject has been defaulted, otherwise a namespace without selectors
	// will get the `Default` selectors, then get copied to byObject and then not get defaulted from
	// byObject, as it already has selectors.
	for namespace, cfg := range opts.DefaultNamespaces {
		cfg = defaultConfig(cfg, optionDefaultsToConfig(&opts))
		if namespace == metav1.NamespaceAll {
			cfg.FieldSelector = fields.AndSelectors(
				appendIfNotNil(
					namespaceAllSelector(maps.Keys(opts.DefaultNamespaces)),
					cfg.FieldSelector,
				)...,
			)
		}
		opts.DefaultNamespaces[namespace] = cfg
	}

	// Default the resync period to 10 hours if unset
	if opts.SyncPeriod == nil {
		opts.SyncPeriod = &defaultSyncPeriod
	}

	if opts.NewInformer == nil {
		opts.NewInformer = toolscache.NewSharedIndexInformer
	}
	return opts, nil
}

func defaultConfig(toDefault, defaultFrom cache.Config) cache.Config {
	if toDefault.LabelSelector == nil {
		toDefault.LabelSelector = defaultFrom.LabelSelector
	}
	if toDefault.FieldSelector == nil {
		toDefault.FieldSelector = defaultFrom.FieldSelector
	}
	if toDefault.Transform == nil {
		toDefault.Transform = defaultFrom.Transform
	}
	if toDefault.UnsafeDisableDeepCopy == nil {
		toDefault.UnsafeDisableDeepCopy = defaultFrom.UnsafeDisableDeepCopy
	}

	return toDefault
}

func namespaceAllSelector(namespaces []string) []fields.Selector {
	selectors := make([]fields.Selector, 0, len(namespaces)-1)
	sort.Strings(namespaces)
	for _, namespace := range namespaces {
		if namespace != metav1.NamespaceAll {
			selectors = append(selectors, fields.OneTermNotEqualSelector("metadata.namespace", namespace))
		}
	}

	return selectors
}

func appendIfNotNil[T comparable](a []T, b T) []T {
	if b != *new(T) {
		return append(a, b)
	}
	return a
}
