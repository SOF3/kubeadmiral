/*
Copyright 2023 The KubeAdmiral Authors.

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

package labelindexer

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
)

type IndexInformerWrapper struct {
	informer cache.SharedIndexInformer
	indexer  *Indexer
}

func NewIndexInformerWrapper(informer cache.SharedIndexInformer) *IndexInformerWrapper {
	indexer := New()

	observe := func(obj any, isDelete bool) {
		object, err := meta.Accessor(obj)
		if err != nil {
			panic(fmt.Errorf("label indexer cannot be used on informer with fallible object accessors: %w", err))
		}

		var labels labels.Set
		if !isDelete {
			labels = object.GetLabels()
		}

		key, err := cache.MetaNamespaceKeyFunc(object)
		if err != nil {
			panic("MetaNamespaceKeyFunc cannot return error on an accessor")
		}
		indexer.Set(key, labels)
	}

	informer.AddEventHandler(&cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(old any) {
			if deleted, ok := old.(cache.DeletedFinalStateUnknown); ok {
				// This object might be stale but ok for our current usage.
				old = deleted.Obj
				if old == nil {
					return
				}
			}
			observe(old, true)
		},
		AddFunc: func(cur any) {
			observe(cur, false)
		},
		UpdateFunc: func(old, cur any) {
			observe(cur, false)
		},
	})

	return &IndexInformerWrapper{informer: informer, indexer: indexer}
}

func (w *IndexInformerWrapper) List(selector labels.Selector, collector ObjectCollector) {
	w.indexer.Find(selector, collector)
}

func (w *IndexInformerWrapper) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return w.informer.AddEventHandler(handler)
}

func (w *IndexInformerWrapper) AddEventHandlerWithResyncPeriod(
	handler cache.ResourceEventHandler,
	resyncPeriod time.Duration,
) (cache.ResourceEventHandlerRegistration, error) {
	return w.informer.AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
}

func (w *IndexInformerWrapper) RemoveEventHandler(handle cache.ResourceEventHandlerRegistration) error {
	return w.informer.RemoveEventHandler(handle)
}
func (w *IndexInformerWrapper) GetStore() cache.Store           { return w.informer.GetStore() }
func (w *IndexInformerWrapper) GetController() cache.Controller { return w.informer.GetController() }
func (w *IndexInformerWrapper) Run(stopCh <-chan struct{})      { w.informer.Run(stopCh) }
func (w *IndexInformerWrapper) HasSynced() bool                 { return w.informer.HasSynced() }
func (w *IndexInformerWrapper) LastSyncResourceVersion() string {
	return w.informer.LastSyncResourceVersion()
}

func (w *IndexInformerWrapper) SetWatchErrorHandler(handler cache.WatchErrorHandler) error {
	return w.informer.SetWatchErrorHandler(handler)
}

func (w *IndexInformerWrapper) SetTransform(handler cache.TransformFunc) error {
	return w.informer.SetTransform(handler)
}
func (w *IndexInformerWrapper) IsStopped() bool { return w.informer.IsStopped() }
func (w *IndexInformerWrapper) AddIndexers(indexers cache.Indexers) error {
	return w.informer.AddIndexers(indexers)
}
func (w *IndexInformerWrapper) GetIndexer() cache.Indexer { return w.informer.GetIndexer() }
