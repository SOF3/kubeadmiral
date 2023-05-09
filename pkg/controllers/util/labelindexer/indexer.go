/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

This file may have been modified by The KubeAdmiral Authors
("KubeAdmiral Modifications"). All KubeAdmiral Modifications
are Copyright 2023 The KubeAdmiral Authors.
*/

package labelindexer

import (
	"sync"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/sets"
)

type Indexer struct {
	mu          sync.RWMutex
	knownLabels map[string]labels.Set
	data        map[string]*labelKeyData
}

type labelKeyData struct {
	values      map[string]sets.String
	cardinality int
}

func New() *Indexer {
	return &Indexer{
		knownLabels: make(map[string]labels.Set),
		data:        make(map[string]*labelKeyData),
	}
}

func (indexer *Indexer) KnownLabelsOf(objectKey string) labels.Set {
	indexer.mu.RLock()
	defer indexer.mu.RUnlock()

	return indexer.knownLabels[objectKey]
}

func (indexer *Indexer) Unset(objectKey string) {
	indexer.Set(objectKey, nil)
}

// Updates the labels of an object.
// If `newLabels == nil`, the object is treated as deleted.
// If `newLabels != nil && len(newLabels) == 0`, the object exists but has no labels.
func (indexer *Indexer) Set(objectKey string, newLabels labels.Set) {
	indexer.mu.Lock()
	defer indexer.mu.Unlock()

	oldLabels := indexer.knownLabels[objectKey]
	if newLabels != nil {
		newLabelsCopy := make(labels.Set, len(newLabels))
		for key, value := range newLabels {
			newLabelsCopy[key] = value
		}
		indexer.knownLabels[objectKey] = newLabelsCopy
	} else {
		delete(indexer.knownLabels, objectKey)
	}

	indexer.updateUnsafe(objectKey, oldLabels, newLabels)
}

func (indexer *Indexer) updateUnsafe(objectKey string, oldLabels, newLabels labels.Set) {
	for labelKey, oldValue := range oldLabels {
		if newValue, isReplace := newLabels[labelKey]; isReplace {
			if oldValue != newValue {
				indexer.removeEntryUnsafe(objectKey, labelKey, oldValue)
				indexer.addEntryUnsafe(objectKey, labelKey, newValue)
			}
		} else {
			indexer.removeEntryUnsafe(objectKey, labelKey, oldValue)
		}
	}

	for labelKey, newValue := range newLabels {
		if _, isReplace := oldLabels[labelKey]; !isReplace {
			indexer.addEntryUnsafe(objectKey, labelKey, newValue)
		}
	}
}

func (indexer *Indexer) addEntryUnsafe(objectKey, labelKey, labelValue string) {
	keyData, ok := indexer.data[labelKey]
	if !ok {
		keyData = &labelKeyData{values: map[string]sets.String{}}
		indexer.data[labelKey] = keyData
	}

	valueData, ok := keyData.values[labelValue]
	if !ok {
		valueData = sets.String{}
		keyData.values[labelValue] = valueData
	}

	if !valueData.Has(objectKey) {
		valueData.Insert(objectKey)
		keyData.cardinality += 1
	}
}

func (indexer *Indexer) removeEntryUnsafe(objectKey, labelKey, labelValue string) {
	keyData := indexer.data[labelKey]
	if keyData == nil {
		return
	}

	valueData := keyData.values[labelValue]
	if valueData.Has(objectKey) {
		valueData.Delete(objectKey)
		keyData.cardinality -= 1
	}

	if valueData.Len() == 0 {
		delete(keyData.values, labelValue)
	}

	if len(keyData.values) == 0 {
		delete(indexer.data, labelKey)
	}
}

type ObjectCollector = func(objectKey string)

type SliceCollector []string

func (collector *SliceCollector) Collect(objectKey string) {
	*collector = append(*collector, objectKey)
}

func (indexer *Indexer) Find(selector labels.Selector, collector ObjectCollector) {
	indexer.mu.RLock()
	defer indexer.mu.RUnlock()
}

func (indexer *Indexer) findUnsafe(selector labels.Selector, collector ObjectCollector) (usedKey string) {
	reqs, selectable := selector.Requirements()
	if !selectable {
		return
	}

	if len(reqs) == 0 {
		for object := range indexer.knownLabels {
			collector(object)
		}
		return
	}

	var bestCost int
	var bestSearcher searcher
	var bestKey string

	for i, req := range reqs {
		cost, searcher := indexer.getSearcherUnsafe(req)
		if i == 0 || cost < bestCost {
			bestCost, bestSearcher, bestKey = cost, searcher, req.Key()
		}
	}

	bestSearcher(func(objectKey string) {
		labels := indexer.knownLabels[objectKey]
		if selector.Matches(labels) {
			collector(objectKey)
		}
	})

	return bestKey
}

type searcher = func(ObjectCollector)

// Returns the cost of linear search.
//
// The "estimated cost" is the number of objects to be searched, which may be greater than the actual match of `req`.
// The returned searcher has the lifetime of the current indexer immutable borrow.
//
// The time complexity of this function is O(len(req.Values())),
// while the time complexity of the returned searcher is O(estimatedCost).
func (indexer *Indexer) getSearcherUnsafe(req labels.Requirement) (estimatedCost int, searcher searcher) {
	switch req.Operator() {
	case selection.Exists:
		keyData := indexer.data[req.Key()]
		if keyData == nil {
			return 0, func(oc ObjectCollector) {}
		}

		return keyData.cardinality, func(collector ObjectCollector) {
			// note that it is impossible for two different values to have the same object, so we do not need to dedup `ret`
			for _, objects := range keyData.values {
				for object := range objects {
					collector(object)
				}
			}
		}
	case selection.In, selection.Equals, selection.DoubleEquals:
		keyData := indexer.data[req.Key()]
		if keyData == nil {
			return 0, func(oc ObjectCollector) {}
		}

		count := int(0)
		for value := range req.Values() {
			// different values have disjoint value set, so the lengths can be summed directly
			count += keyData.values[value].Len()
		}

		return count, func(collector ObjectCollector) {
			for value := range req.Values() {
				objects := keyData.values[value]
				for object := range objects {
					collector(object)
				}
			}
		}
	default: // including DoesNotExist, NotEquals, GreaterThan, LessThan
		// unsupported operator, perform full linear search
		return len(indexer.knownLabels), func(collector ObjectCollector) {
			for object := range indexer.knownLabels {
				collector(object)
			}
		}
	}
}
