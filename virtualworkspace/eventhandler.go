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

package virtualworkspace

import (
	"context"
	"reflect"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kcp-dev/logicalcluster/v3"
	mcreconcile "github.com/multicluster-runtime/multicluster-runtime/pkg/reconcile"
)

func EventHandlerFunc(string, cluster.Cluster) handler.TypedEventHandler[client.Object, mcreconcile.Request] {
	return &EnqueueRequestForObject{}
}

var _ handler.TypedEventHandler[client.Object, mcreconcile.Request] = &EnqueueRequestForObject{}

type EnqueueRequestForObject = TypedEnqueueRequestForObject[client.Object]

type TypedEnqueueRequestForObject[object client.Object] struct{}

// Create implements EventHandler.
func (e *TypedEnqueueRequestForObject[T]) Create(ctx context.Context, evt event.TypedCreateEvent[T], q workqueue.TypedRateLimitingInterface[mcreconcile.Request]) {
	if isNil(evt.Object) {
		return
	}
	q.Add(mcreconcile.Request{
		ClusterName: string(logicalcluster.From(evt.Object)),
		Request: reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      evt.Object.GetName(),
				Namespace: evt.Object.GetNamespace(),
			},
		}})
}

// Update implements EventHandler.
func (e *TypedEnqueueRequestForObject[T]) Update(ctx context.Context, evt event.TypedUpdateEvent[T], q workqueue.TypedRateLimitingInterface[mcreconcile.Request]) {
	switch {
	case !isNil(evt.ObjectNew):
		q.Add(mcreconcile.Request{
			ClusterName: string(logicalcluster.From(evt.ObjectNew)),
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      evt.ObjectNew.GetName(),
					Namespace: evt.ObjectNew.GetNamespace(),
				},
			}})
	case !isNil(evt.ObjectOld):
		q.Add(mcreconcile.Request{
			ClusterName: string(logicalcluster.From(evt.ObjectOld)),
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      evt.ObjectOld.GetName(),
					Namespace: evt.ObjectOld.GetNamespace(),
				},
			}})
	default:
	}
}

// Delete implements EventHandler.
func (e *TypedEnqueueRequestForObject[T]) Delete(ctx context.Context, evt event.TypedDeleteEvent[T], q workqueue.TypedRateLimitingInterface[mcreconcile.Request]) {
	if isNil(evt.Object) {
		return
	}
	q.Add(mcreconcile.Request{
		ClusterName: string(logicalcluster.From(evt.Object)),
		Request: reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      evt.Object.GetName(),
				Namespace: evt.Object.GetNamespace(),
			},
		}})
}

// Generic implements EventHandler.
func (e *TypedEnqueueRequestForObject[T]) Generic(ctx context.Context, evt event.TypedGenericEvent[T], q workqueue.TypedRateLimitingInterface[mcreconcile.Request]) {
	if isNil(evt.Object) {
		return
	}
	q.Add(mcreconcile.Request{
		ClusterName: string(logicalcluster.From(evt.Object)),
		Request: reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      evt.Object.GetName(),
				Namespace: evt.Object.GetNamespace(),
			},
		}})
}

func isNil(arg any) bool {
	if v := reflect.ValueOf(arg); !v.IsValid() || ((v.Kind() == reflect.Ptr ||
		v.Kind() == reflect.Interface ||
		v.Kind() == reflect.Slice ||
		v.Kind() == reflect.Map ||
		v.Kind() == reflect.Chan ||
		v.Kind() == reflect.Func) && v.IsNil()) {
		return true
	}
	return false
}
