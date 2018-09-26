package machinedeployment

import (
	"github.com/golang/glog"

	"k8s.io/apimachinery/pkg/api/equality"

	"sigs.k8s.io/cluster-api/pkg/apis/cluster/v1alpha1"
)

func (c *MachineDeploymentControllerImpl) ensureStatus(md *v1alpha1.MachineDeployment, newMS *v1alpha1.MachineSet, oldMSs []*v1alpha1.MachineSet) (*v1alpha1.MachineDeployment, error) {

	newStatus := v1alpha1.MachineDeploymentStatus{ObservedGeneration: md.Generation,
		UpdatedReplicas: newMS.Status.Replicas}

	allOwnedMS := append(oldMSs, newMS)
	for _, ms := range allOwnedMS {
		newStatus.Replicas = newStatus.Replicas + ms.Status.Replicas
		newStatus.ReadyReplicas = newStatus.ReadyReplicas + ms.Status.ReadyReplicas
		newStatus.AvailableReplicas = newStatus.AvailableReplicas + ms.Status.AvailableReplicas
	}

	newStatus.UnavailableReplicas = *md.Spec.Replicas - newStatus.AvailableReplicas
	if newStatus.UnavailableReplicas < 0 {
		newStatus.UnavailableReplicas = 0
	}

	if !equality.Semantic.DeepEqual(md.Status, newStatus) {
		md.Status = newStatus
		glog.V(4).Infof("Updating status of machineDeployment %s/%s", md.Namespace, md.Name)
		return c.machineClient.ClusterV1alpha1().MachineDeployments(md.Namespace).UpdateStatus(md)
	}

	return md, nil
}
