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

package machinedeployment

import (
	"fmt"
	"sort"

	"github.com/golang/glog"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/integer"

	"sigs.k8s.io/cluster-api/pkg/apis/cluster/v1alpha1"
	dutil "sigs.k8s.io/cluster-api/pkg/controller/machinedeployment/util"
)

// rolloutRolling implements the logic for rolling a new machine set.
func (dc *MachineDeploymentControllerImpl) rolloutRolling(d *v1alpha1.MachineDeployment, msList []*v1alpha1.MachineSet, machineMap map[types.UID]*v1alpha1.MachineList) error {
	newMS, oldMSs, err := dc.getAllMachineSetsAndSyncRevision(d, msList, machineMap, true)
	if err != nil {
		return err
	}
	allMSs := append(oldMSs, newMS)

	// Scale up, if we can.
	newMS, err = dc.reconcileNewMachineSet(allMSs, newMS, d)
	if err != nil {
		return err
	}

	// Scale down, if we can.
	oldMSs, err = dc.reconcileOldMachineSets(allMSs, oldMSs, newMS, d)
	if err != nil {
		return err
	}

	if dutil.DeploymentComplete(d, &d.Status) {
		if err := dc.cleanupDeployment(oldMSs, d); err != nil {
			return err
		}
	}

	_, err = dc.ensureStatus(d, newMS, oldMSs)
	return err
}

func (dc *MachineDeploymentControllerImpl) reconcileNewMachineSet(allMSs []*v1alpha1.MachineSet, newMS *v1alpha1.MachineSet, deployment *v1alpha1.MachineDeployment) (*v1alpha1.MachineSet, error) {
	if deployment.Spec.Replicas == nil {
		return newMS, fmt.Errorf("spec replicas for deployment set %v is nil, this is unexpected", deployment.Name)
	}
	if newMS.Spec.Replicas == nil {
		return newMS, fmt.Errorf("spec replicas for machine set %v is nil, this is unexpected", newMS.Name)
	}

	var err error
	if *(newMS.Spec.Replicas) == *(deployment.Spec.Replicas) {
		// Scaling not required.
		return newMS, nil
	}
	if *(newMS.Spec.Replicas) > *(deployment.Spec.Replicas) {
		// Scale down.
		_, newMS, err = dc.scaleMachineSet(newMS, *(deployment.Spec.Replicas), deployment)
		return newMS, err
	}
	newReplicasCount, err := dutil.NewMSNewReplicas(deployment, allMSs, newMS)
	if err != nil {
		return newMS, err
	}
	_, newMS, err = dc.scaleMachineSet(newMS, newReplicasCount, deployment)
	return newMS, err
}

func (dc *MachineDeploymentControllerImpl) reconcileOldMachineSets(allMSs []*v1alpha1.MachineSet, oldMSs []*v1alpha1.MachineSet, newMS *v1alpha1.MachineSet, deployment *v1alpha1.MachineDeployment) ([]*v1alpha1.MachineSet, error) {
	if deployment.Spec.Replicas == nil {
		return oldMSs, fmt.Errorf("spec replicas for deployment set %v is nil, this is unexpected", deployment.Name)
	}
	if newMS.Spec.Replicas == nil {
		return oldMSs, fmt.Errorf("spec replicas for machine set %v is nil, this is unexpected", newMS.Name)
	}

	oldMachinesCount := dutil.GetReplicaCountForMachineSets(oldMSs)
	if oldMachinesCount == 0 {
		// Can't scale down further
		return oldMSs, nil
	}

	allMachinesCount := dutil.GetReplicaCountForMachineSets(allMSs)
	glog.V(4).Infof("New machine set %s/%s has %d available machines.", newMS.Namespace, newMS.Name, newMS.Status.AvailableReplicas)
	maxUnavailable := dutil.MaxUnavailable(*deployment)

	// Check if we can scale down. We can scale down in the following 2 cases:
	// * Some old machine sets have unhealthy replicas, we could safely scale down those unhealthy replicas since that won't further
	//  increase unavailability.
	// * New machine set has scaled up and it's replicas becomes ready, then we can scale down old machine sets in a further step.
	//
	// maxScaledDown := allMachinesCount - minAvailable - newMachineSetMachinesUnavailable
	// take into account not only maxUnavailable and any surge machines that have been created, but also unavailable machines from
	// the newMS, so that the unavailable machines from the newMS would not make us scale down old machine sets in a further
	// step(that will increase unavailability).
	//
	// Concrete example:
	//
	// * 10 replicas
	// * 2 maxUnavailable (absolute number, not percent)
	// * 3 maxSurge (absolute number, not percent)
	//
	// case 1:
	// * Deployment is updated, newMS is created with 3 replicas, oldMS is scaled down to 8, and newMS is scaled up to 5.
	// * The new machine set machines crashloop and never become available.
	// * allMachinesCount is 13. minAvailable is 8. newMSMachinesUnavailable is 5.
	// * A node fails and causes one of the oldMS machines to become unavailable. However, 13 - 8 - 5 = 0, so the oldMS won't be scaled down.
	// * The user notices the crashloop and does kubectl rollout undo to rollback.
	// * newMSMachinesUnavailable is 1, since we rolled back to the good machine set, so maxScaledDown = 13 - 8 - 1 = 4. 4 of the crashlooping machines will be scaled down.
	// * The total number of machines will then be 9 and the newMS can be scaled up to 10.
	//
	// case 2:
	// Same example, but pushing a new machine template instead of rolling back (aka "roll over"):
	// * The new machine set created must start with 0 replicas because allMachinesCount is already at 13.
	// * However, newMSMachinesUnavailable would also be 0, so the 2 old machine sets could be scaled down by 5 (13 - 8 - 0), which would then
	// allow the new machine set to be scaled up by 5.
	minAvailable := *(deployment.Spec.Replicas) - maxUnavailable
	newMSUnavailableMachineCount := *(newMS.Spec.Replicas) - newMS.Status.AvailableReplicas
	maxScaledDown := allMachinesCount - minAvailable - newMSUnavailableMachineCount
	if maxScaledDown <= 0 {
		return oldMSs, nil
	}

	// Clean up unhealthy replicas first, otherwise unhealthy replicas will block deployment
	// and cause timeout. See https://github.com/kubernetes/kubernetes/issues/16737
	oldMSs, cleanupCount, err := dc.cleanupUnhealthyReplicas(oldMSs, deployment, maxScaledDown)
	if err != nil {
		return oldMSs, nil
	}
	glog.V(4).Infof("Cleaned up unhealthy replicas from old MSes by %d", cleanupCount)

	// Scale down old machine sets, need check maxUnavailable to ensure we can scale down
	allMSs = append(oldMSs, newMS)
	scaledDownCount, oldMSs, err := dc.scaleDownOldMachineSetsForRollingUpdate(allMSs, oldMSs, deployment)
	if err != nil {
		return oldMSs, err
	}
	glog.V(4).Infof("Scaled down old MSes of deployment %s by %d", deployment.Name, scaledDownCount)

	return oldMSs, nil
}

// cleanupUnhealthyReplicas will scale down old machine sets with unhealthy replicas, so that all unhealthy replicas will be deleted.
func (dc *MachineDeploymentControllerImpl) cleanupUnhealthyReplicas(oldMSs []*v1alpha1.MachineSet, deployment *v1alpha1.MachineDeployment, maxCleanupCount int32) ([]*v1alpha1.MachineSet, int32, error) {
	sort.Sort(dutil.MachineSetsByCreationTimestamp(oldMSs))
	// Safely scale down all old machine sets with unhealthy replicas. Replica set will sort the machines in the order
	// such that not-ready < ready, unscheduled < scheduled, and pending < running. This ensures that unhealthy replicas will
	// been deleted first and won't increase unavailability.
	totalScaledDown := int32(0)
	for i, targetMS := range oldMSs {
		if targetMS.Spec.Replicas == nil {
			return nil, 0, fmt.Errorf("spec replicas for machine set %v is nil, this is unexpected", targetMS.Name)
		}

		if totalScaledDown >= maxCleanupCount {
			break
		}
		oldMSReplicas := *(targetMS.Spec.Replicas)
		if oldMSReplicas == 0 {
			// cannot scale down this machine set.
			continue
		}
		oldMSAvailableReplicas := targetMS.Status.AvailableReplicas
		glog.V(4).Infof("Found %d available machines in old MS %s/%s", oldMSAvailableReplicas, targetMS.Namespace, targetMS.Name)
		if oldMSReplicas == oldMSAvailableReplicas {
			// no unhealthy replicas found, no scaling required.
			continue
		}

		remainingCleanupCount := maxCleanupCount - totalScaledDown
		unhealthyCount := oldMSReplicas - oldMSAvailableReplicas
		scaledDownCount := integer.Int32Min(remainingCleanupCount, unhealthyCount)
		newReplicasCount := oldMSReplicas - scaledDownCount

		if newReplicasCount > oldMSReplicas {
			return nil, 0, fmt.Errorf("when cleaning up unhealthy replicas, got invalid request to scale down %s/%s %d -> %d", targetMS.Namespace, targetMS.Name, oldMSReplicas, newReplicasCount)
		}
		_, updatedOldMS, err := dc.scaleMachineSet(targetMS, newReplicasCount, deployment)
		if err != nil {
			return nil, totalScaledDown, err
		}
		totalScaledDown += scaledDownCount
		oldMSs[i] = updatedOldMS
	}
	return oldMSs, totalScaledDown, nil
}

// scaleDownOldMachineSetsForRollingUpdate scales down old machine sets when deployment strategy is "RollingUpdate".
// Need check maxUnavailable to ensure availability
func (dc *MachineDeploymentControllerImpl) scaleDownOldMachineSetsForRollingUpdate(allMSs []*v1alpha1.MachineSet, oldMSs []*v1alpha1.MachineSet, deployment *v1alpha1.MachineDeployment) (int32, []*v1alpha1.MachineSet, error) {
	if deployment.Spec.Replicas == nil {
		return 0, oldMSs, fmt.Errorf("spec replicas for deployment %v is nil, this is unexpected", deployment.Name)
	}

	maxUnavailable := dutil.MaxUnavailable(*deployment)

	// Check if we can scale down.
	minAvailable := *(deployment.Spec.Replicas) - maxUnavailable
	// Find the number of available machines.
	availableMachineCount := dutil.GetAvailableReplicaCountForMachineSets(allMSs)
	if availableMachineCount <= minAvailable {
		// Cannot scale down.
		return 0, oldMSs, nil
	}
	glog.V(4).Infof("Found %d available machines in deployment %s, scaling down old MSes", availableMachineCount, deployment.Name)

	sort.Sort(dutil.MachineSetsByCreationTimestamp(oldMSs))

	totalScaledDown := int32(0)
	totalScaleDownCount := availableMachineCount - minAvailable
	var err error
	for idx, _ := range oldMSs {
		if oldMSs[idx].Spec.Replicas == nil {
			return 0, oldMSs, fmt.Errorf("spec replicas for machine set %v is nil, this is unexpected", oldMSs[idx].Name)
		}

		if totalScaledDown >= totalScaleDownCount {
			// No further scaling required.
			break
		}
		if *(oldMSs[idx].Spec.Replicas) == 0 {
			// cannot scale down this MachineSet.
			continue
		}
		// Scale down.
		scaleDownCount := int32(integer.Int32Min(*(oldMSs[idx].Spec.Replicas), totalScaleDownCount-totalScaledDown))
		newReplicasCount := *(oldMSs[idx].Spec.Replicas) - scaleDownCount
		if newReplicasCount > *(oldMSs[idx].Spec.Replicas) {
			return totalScaledDown, oldMSs, fmt.Errorf("when scaling down old MS, got invalid request to scale down %s/%s %d -> %d", oldMSs[idx].Namespace, oldMSs[idx].Name, *(oldMSs[idx].Spec.Replicas), newReplicasCount)
		}
		_, oldMSs[idx], err = dc.scaleMachineSet(oldMSs[idx], newReplicasCount, deployment)
		if err != nil {
			return totalScaledDown, oldMSs, err
		}

		totalScaledDown += scaleDownCount
	}

	return totalScaledDown, oldMSs, nil
}
