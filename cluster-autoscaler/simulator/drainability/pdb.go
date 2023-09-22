/*
Copyright 2023 The Kubernetes Authors.

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

package drainability

import (
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/pdb"
	"k8s.io/autoscaler/cluster-autoscaler/utils/drain"
)

// PdbRule is a drainability rule on how to handle pods with pdbs.
type PdbRule struct {
	tracker pdb.RemainingPdbTracker
}

// NewPdbRule creates a new PdbRule.
func NewPdbRule(tracker pdb.RemainingPdbTracker) *PdbRule {
	return &PdbRule{tracker}
}

// Drainable decides how to handle pods with pdbs on node drain.
func (r *PdbRule) Drainable(pod *apiv1.Pod) Status {
	if r.tracker == nil {
		return NewUndefinedStatus()
	}

	// TODO: Replace this logic with RemainingPdbTracker.CanRemovePods()
	// after deprecating legacy scale down.
	for _, pdb := range r.tracker.GetPdbs() {
		selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil {
			return NewBlockedStatus(drain.UnexpectedError, fmt.Errorf("failed to convert label selector"))
		}

		if pod.Namespace == pdb.Namespace && selector.Matches(labels.Set(pod.Labels)) && pdb.Status.DisruptionsAllowed < 1 {
			return NewBlockedStatus(drain.NotEnoughPdb, fmt.Errorf("not enough pod disruption budget to move %s/%s", pod.Namespace, pod.Name))
		}
	}
	return NewUndefinedStatus()
}
