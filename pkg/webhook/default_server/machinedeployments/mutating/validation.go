package mutating

import (
	"log"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/cluster-api/pkg/apis/cluster/common"
	"sigs.k8s.io/cluster-api/pkg/apis/cluster/v1alpha1"
)

// Validate checks that an instance of MachineDeployment is well formed
func Validate(obj runtime.Object) field.ErrorList {
	md := obj.(*v1alpha1.MachineDeployment)
	log.Printf("Validating fields for MachineDeployment %s\n", md.Name)
	allErrs := field.ErrorList{}

	allErrs = append(allErrs, ValidateMachineDeploymentSpec(&md.Spec, field.NewPath("spec"))...)
	return allErrs
}

func ValidateMachineDeploymentSpec(spec *v1alpha1.MachineDeploymentSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, metav1validation.ValidateLabelSelector(&spec.Selector, fldPath.Child("selector"))...)
	if len(spec.Selector.MatchLabels)+len(spec.Selector.MatchExpressions) == 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("selector"), spec.Selector, "empty selector is not valid for MachineSet."))
	}
	selector, err := metav1.LabelSelectorAsSelector(&spec.Selector)
	if err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("selector"), spec.Selector, "invalid label selector."))
	} else {
		labels := labels.Set(spec.Template.Labels)
		if !selector.Matches(labels) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("template", "metadata", "labels"), spec.Template.Labels, "`selector` does not match template `labels`"))
		}
	}

	if spec.Replicas == nil || *spec.Replicas < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("replicas"), *spec.Replicas, "replicas must be specified and can not be negative"))
	}

	allErrs = append(allErrs, ValidateMachineDeploymentStrategy(&spec.Strategy, fldPath.Child("strategy"))...)
	return allErrs
}

func ValidateMachineDeploymentStrategy(strategy *v1alpha1.MachineDeploymentStrategy, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	switch strategy.Type {
	case common.RollingUpdateMachineDeploymentStrategyType:
		if strategy.RollingUpdate != nil {
			allErrs = append(allErrs, ValidateMachineRollingUpdateDeployment(strategy.RollingUpdate, fldPath.Child("rollingUpdate"))...)
		}
	default:
		allErrs = append(allErrs, field.Invalid(fldPath.Child("Type"), strategy.Type, "is an invalid type"))
	}
	return allErrs
}

func ValidateMachineRollingUpdateDeployment(rollingUpdate *v1alpha1.MachineRollingUpdateDeployment, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	var maxUnavailable int
	var maxSurge int

	if rollingUpdate.MaxUnavailable != nil {
		allErrs = append(allErrs, ValidatePositiveIntOrPercent(rollingUpdate.MaxUnavailable, fldPath.Child("maxUnavailable"))...)
		maxUnavailable, _ = getIntOrPercent(rollingUpdate.MaxUnavailable, false)

		// Validate that MaxUnavailable is not more than 100%.
		if len(utilvalidation.IsValidPercent(rollingUpdate.MaxUnavailable.StrVal)) == 0 && maxUnavailable > 100 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("maxUnavailable"), rollingUpdate.MaxUnavailable, "should not be more than 100%"))
		}
	}

	if rollingUpdate.MaxSurge != nil {
		allErrs = append(allErrs, ValidatePositiveIntOrPercent(rollingUpdate.MaxSurge, fldPath.Child("maxSurge"))...)
		maxSurge, _ = getIntOrPercent(rollingUpdate.MaxSurge, true)
	}

	if rollingUpdate.MaxUnavailable != nil && rollingUpdate.MaxSurge != nil && maxUnavailable == 0 && maxSurge == 0 {
		// Both MaxSurge and MaxUnavailable cannot be zero.
		allErrs = append(allErrs, field.Invalid(fldPath.Child("maxUnavailable"), rollingUpdate.MaxUnavailable, "may not be 0 when `maxSurge` is 0"))
	}

	return allErrs
}

func ValidatePositiveIntOrPercent(s *intstr.IntOrString, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if x, err := getIntOrPercent(s, false); err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, s.StrVal, "value should be int(5) or percentage(5%)"))
	} else if x < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath, x, "value should not be negative"))
	}
	return allErrs
}

func getIntOrPercent(s *intstr.IntOrString, roundUp bool) (int, error) {
	return intstr.GetValueFromIntOrPercent(s, 100, roundUp)
}

// DefaultingFunction sets default MachineDeployment field values
func DefaultingFunction(o interface{}) {
	obj := o.(*v1alpha1.MachineDeployment)
	// set default field values here
	log.Printf("Defaulting fields for MachineDeployment %s\n", obj.Name)
	if obj.Spec.Replicas == nil {
		obj.Spec.Replicas = new(int32)
		*obj.Spec.Replicas = 1
	}

	if obj.Spec.MinReadySeconds == nil {
		obj.Spec.MinReadySeconds = new(int32)
		*obj.Spec.MinReadySeconds = 0
	}

	if obj.Spec.RevisionHistoryLimit == nil {
		obj.Spec.RevisionHistoryLimit = new(int32)
		*obj.Spec.RevisionHistoryLimit = 1
	}

	if obj.Spec.ProgressDeadlineSeconds == nil {
		obj.Spec.ProgressDeadlineSeconds = new(int32)
		*obj.Spec.ProgressDeadlineSeconds = 600
	}

	if obj.Spec.Strategy.Type == "" {
		obj.Spec.Strategy.Type = common.RollingUpdateMachineDeploymentStrategyType
	}

	// Default RollingUpdate strategy only if strategy type is RollingUpdate.
	if obj.Spec.Strategy.Type == common.RollingUpdateMachineDeploymentStrategyType {
		if obj.Spec.Strategy.RollingUpdate == nil {
			obj.Spec.Strategy.RollingUpdate = &v1alpha1.MachineRollingUpdateDeployment{}
		}

		if obj.Spec.Strategy.RollingUpdate.MaxSurge == nil {
			x := intstr.FromInt(1)
			obj.Spec.Strategy.RollingUpdate.MaxSurge = &x
		}

		if obj.Spec.Strategy.RollingUpdate.MaxUnavailable == nil {
			x := intstr.FromInt(0)
			obj.Spec.Strategy.RollingUpdate.MaxUnavailable = &x
		}
	}

	if len(obj.Namespace) == 0 {
		obj.Namespace = metav1.NamespaceDefault
	}
}
