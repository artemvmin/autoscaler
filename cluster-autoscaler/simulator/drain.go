/*
Copyright 2015 The Kubernetes Authors.

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

package simulator

import (
	"time"

	apiv1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/pdb"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability"
	"k8s.io/autoscaler/cluster-autoscaler/utils/drain"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	pod_util "k8s.io/autoscaler/cluster-autoscaler/utils/pod"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

// NodeDeleteOptions contains various options to customize how draining will behave
type NodeDeleteOptions struct {
	// SkipNodesWithSystemPods tells if nodes with pods from kube-system should be deleted (except for DaemonSet or mirror pods)
	SkipNodesWithSystemPods bool
	// SkipNodesWithLocalStorage tells if nodes with pods with local storage, e.g. EmptyDir or HostPath, should be deleted
	SkipNodesWithLocalStorage bool
	// SkipNodesWithCustomControllerPods tells if nodes with custom-controller owned pods should be skipped from deletion (skip if 'true')
	SkipNodesWithCustomControllerPods bool
	// MinReplicaCount controls the minimum number of replicas that a replica set or replication controller should have
	// to allow their pods deletion in scale down
	MinReplicaCount int
	// DrainabilityRules contain a list of checks that are used to verify whether a pod can be drained from node.
	DrainabilityRules []drainability.Rule
}

// NewNodeDeleteOptions returns new node delete options extracted from autoscaling options
func NewNodeDeleteOptions(opts config.AutoscalingOptions, remainingPdbTracker pdb.RemainingPdbTracker) NodeDeleteOptions {
	return NodeDeleteOptions{
		SkipNodesWithSystemPods:           opts.SkipNodesWithSystemPods,
		SkipNodesWithLocalStorage:         opts.SkipNodesWithLocalStorage,
		MinReplicaCount:                   opts.MinReplicaCount,
		SkipNodesWithCustomControllerPods: opts.SkipNodesWithCustomControllerPods,
		DrainabilityRules:                 drainability.DefaultRules(remainingPdbTracker),
	}
}

// GetPodsToMove returns a list of pods that should be moved elsewhere
// and a list of DaemonSet pods that should be evicted if the node
// is drained. Raises error if there is an unreplicated pod.
// Based on kubectl drain code. If listers is nil it makes an assumption that RC, DS, Jobs and RS were deleted
// along with their pods (no abandoned pods with dangling created-by annotation).
// If listers is not nil it checks whether RC, DS, Jobs and RS that created these pods
// still exist.
// TODO(x13n): Rewrite GetPodsForDeletionOnNodeDrain into a set of DrainabilityRules.
func GetPodsToMove(nodeInfo *schedulerframework.NodeInfo, deleteOptions NodeDeleteOptions, listers kube_util.ListerRegistry,
	remainingPdbTracker pdb.RemainingPdbTracker, timestamp time.Time) (pods []*apiv1.Pod, daemonSetPods []*apiv1.Pod, blockingPod *drain.BlockingPod, err error) {
	var drainPods, drainDS []*apiv1.Pod
	drainabilityRules := deleteOptions.DrainabilityRules
	if drainabilityRules == nil {
		// TODO(reviewer comment): This uses the dynamic set of pdbs, while the
		// drainability rules above use the static, global object. This will cause
		// problems in the context of a goroutine. One option is to add a mutex to
		// the pdb object. This assumes that the async node deletion function
		// doesn't care which copy of pdbs it has, as long as its consistent. The
		// other option is to somehow make these drainability rules dynamic (e.g.
		// by passing them a new pdb list). This will take more work.
		drainabilityRules = drainability.DefaultRules(remainingPdbTracker)
	}
	for _, podInfo := range nodeInfo.Pods {
		pod := podInfo.Pod
		d := drainabilityStatus(pod, drainabilityRules)
		switch d.Outcome {
		case drainability.UndefinedOutcome:
			pods = append(pods, podInfo.Pod)
		case drainability.DrainOk:
			if pod_util.IsDaemonSetPod(pod) {
				drainDS = append(drainDS, pod)
			} else {
				drainPods = append(drainPods, pod)
			}
		case drainability.BlockDrain:
			// TODO(reviewer note): can we blame the pod, even though pdb call failed?
			// What is the consequence of returning a pod here?
			// Alternatively, drainability would have to pass a value to indicate pod
			// vs infrastructure error.
			blockingPod = &drain.BlockingPod{
				Pod:    pod,
				Reason: d.BlockingReason,
			}
			err = d.Error
			return
		}
	}

	var pdbs []*policyv1.PodDisruptionBudget
	if remainingPdbTracker != nil {
		pdbs = remainingPdbTracker.GetPdbs()
	}
	pods, daemonSetPods, blockingPod, err = drain.GetPodsForDeletionOnNodeDrain(
		pods,
		pdbs,
		deleteOptions.SkipNodesWithSystemPods,
		deleteOptions.SkipNodesWithLocalStorage,
		deleteOptions.SkipNodesWithCustomControllerPods,
		listers,
		int32(deleteOptions.MinReplicaCount),
		timestamp)
	pods = append(pods, drainPods...)
	daemonSetPods = append(daemonSetPods, drainDS...)
	if err != nil {
		return pods, daemonSetPods, blockingPod, err
	}

	return pods, daemonSetPods, nil, nil
}

func drainabilityStatus(pod *apiv1.Pod, dr []drainability.Rule) drainability.Status {
	for _, f := range dr {
		if d := f.Drainable(pod); d.Outcome != drainability.UndefinedOutcome {
			return d
		}
	}
	return drainability.Status{
		Outcome: drainability.UndefinedOutcome,
	}
}
