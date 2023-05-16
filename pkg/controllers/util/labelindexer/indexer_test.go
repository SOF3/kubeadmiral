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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
)

type input struct {
	name string
}

func TestSet(t *testing.T) {
	assert := assert.New(t)

	indexer := New()
	indexer.Set("foo", labels.Set{"k1": "v11", "k2": "v2"})

	assert.Len(indexer.knownLabels, 1)
	assert.Len(indexer.data, 2)
	assert.Len(indexer.data["k1"].values, 1)
	assert.Equal(1, indexer.data["k1"].cardinality)
	assert.Len(indexer.data["k1"].values["v11"], 1)
	assert.Contains(indexer.data["k1"].values["v11"], "foo")

	// add object with one shared key and distinct value
	indexer.Set("bar", labels.Set{"k1": "v12", "k3": "v3"})

	assert.Len(indexer.knownLabels, 2)
	assert.Len(indexer.data, 3)
	assert.Len(indexer.data["k1"].values, 2)
	assert.Equal(2, indexer.data["k1"].cardinality)
	assert.Len(indexer.data["k1"].values["v12"], 1)
	assert.Contains(indexer.data["k1"].values["v12"], "bar")

	// remove label k1, duplicate label value of k3
	indexer.Set("foo", labels.Set{"k3": "v3", "k2": "v2"})

	assert.Len(indexer.knownLabels, 2)
	assert.Len(indexer.data, 3)
	assert.Contains(indexer.data, "k1")
	assert.Equal(1, indexer.data["k1"].cardinality)
	assert.Len(indexer.data["k1"].values, 1)
	assert.Contains(indexer.data["k1"].values, "v12")
	assert.Equal(2, indexer.data["k3"].cardinality)
	assert.Len(indexer.data["k3"].values["v3"], 2)
	assert.Contains(indexer.data["k3"].values["v3"], "foo")
	assert.Contains(indexer.data["k3"].values["v3"], "bar")

	// unsets k2, removes one k3=v3 entry
	indexer.Unset("bar")

	assert.Len(indexer.knownLabels, 1)
	assert.Len(indexer.data, 2)
	assert.Contains(indexer.data, "k2")
	assert.Contains(indexer.data, "k3")
	assert.Equal(1, indexer.data["k3"].cardinality)
	assert.Contains(indexer.data["k3"].values["v3"], "foo")
	assert.NotContains(indexer.data["k3"].values["v3"], "bar")
}

func TestFind(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		objects     map[string]labels.Set
		reqs        []labels.Requirement
		bestReqKeys []string
	}{
		{
			name: "three keys with cardinalities 3/2/2, should use either cardinality-2 key",
			objects: map[string]labels.Set{
				"foo": {"A": "a", "B": "b", "C": "c"},
				"bar": {"A": "a", "B": "x", "C": "c"},
				"qux": {"A": "a", "B": "b", "C": "y"},
			},
			reqs: []labels.Requirement{
				mustNewRequirement("A", selection.Equals, "a"),
				mustNewRequirement("B", selection.Equals, "b"),
				mustNewRequirement("C", selection.Equals, "c"),
			},
			bestReqKeys: []string{"B", "C"},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			assert := assert.New(t)

			indexer := New()
			for objectName, objectLabels := range testCase.objects {
				indexer.Set(objectName, objectLabels)
			}

			selector := labels.NewSelector().Add(testCase.reqs...)
			collector := SliceCollector[string]{Transform: func(s string) string { return s }}

			bestKey := indexer.findUnsafe(selector, collector.Collect)

			assert.Contains(testCase.bestReqKeys, bestKey, "best key should be the ones with cardinality 2")

			var expectedResults []string
			for objectName, objectLabels := range testCase.objects {
				if selector.Matches(objectLabels) {
					expectedResults = append(expectedResults, objectName)
				}
			}

			sort.Strings(expectedResults)
			sort.Strings(collector.Slice)
			assert.Equal(expectedResults, []string(collector.Slice))
		})
	}
}

func mustNewRequirement(key string, op selection.Operator, values ...string) labels.Requirement {
	req, err := labels.NewRequirement(key, op, values)
	if err != nil {
		panic(err)
	}

	return *req
}
