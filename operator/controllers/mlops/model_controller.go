/*
Copyright (c) 2024 Seldon Technologies Ltd.

Use of this software is governed by
(1) the license included in the LICENSE file or
(2) if the license included in the LICENSE file is the Business Source License 1.1,
the Change License after the Change Date as each is defined in accordance with the LICENSE file.
*/

package mlops

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	schedulerAPI "github.com/seldonio/seldon-core/apis/go/v2/mlops/scheduler"

	mlopsv1alpha1 "github.com/seldonio/seldon-core/operator/v2/apis/mlops/v1alpha1"
	"github.com/seldonio/seldon-core/operator/v2/pkg/constants"
	"github.com/seldonio/seldon-core/operator/v2/pkg/utils"
)

// ModelReconciler reconciles a Model object
type ModelReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Scheduler SchedulerClient
	Recorder  record.EventRecorder
}

// hasInProgressRollout reports whether the latest model version is still being rolled out while
// an earlier version keeps serving traffic. The scheduler only streams the latest version's state
// to the operator, so AvailableReplicas > 0 reflects replicas attached to the in-flight version and
// a not-Ready ModelReady condition means it has not finished taking over. When a Model is deleted
// during such a rollout (version switch, unready server pod and/or config update arriving together),
// unloading now would let the scheduler drop the old, still-serving version before the new one is
// ready. The decision is conservative: any uncertainty about the rollout state returns false and
// deletion proceeds as before.
func hasInProgressRollout(model *mlopsv1alpha1.Model) bool {
	condition := model.Status.GetCondition(apis.ConditionReady)
	if condition == nil || condition.Status == v1.ConditionTrue {
		return false
	}
	return model.Status.AvailableReplicas > 0
}

func (r *ModelReconciler) handleFinalizer(ctx context.Context, logger logr.Logger, model *mlopsv1alpha1.Model) (bool, ctrl.Result, error) {

	// Check if we are being deleted or not
	if model.ObjectMeta.DeletionTimestamp.IsZero() { // Not being deleted

		// Add our finalizer
		if !utils.ContainsStr(model.ObjectMeta.Finalizers, constants.ModelFinalizerName) {
			model.ObjectMeta.Finalizers = append(model.ObjectMeta.Finalizers, constants.ModelFinalizerName)
			if err := r.Update(ctx, model); err != nil {
				return true, ctrl.Result{}, err
			}
		}
		return false, ctrl.Result{}, nil
	}

	// model is being deleted
	if utils.ContainsStr(model.ObjectMeta.Finalizers, constants.ModelFinalizerName) {
		// Keep the previous model version serving while the new version is still rolling out.
		// The scheduler retains old versions until the latest one is available, but the operator
		// owns the unload request triggered by deletion; deferring it here avoids racing the
		// scheduler cleanup and dropping the only live replicas. Reconciliation is requeued so the
		// unload is retried once the rollout completes.
		if hasInProgressRollout(model) {
			logger.Info(
				"Deferring model unload while a new version is still rolling out",
				"model", model.Name,
				"availableReplicas", model.Status.AvailableReplicas,
			)
			r.Recorder.Eventf(
				model,
				v1.EventTypeNormal,
				"ModelRolloutInProgress",
				"Deferring unload of Model %q: a new version is still rolling out",
				model.Name,
			)
			return true, ctrl.Result{RequeueAfter: constants.ModelRolloutRequeueInterval}, nil
		}

		// Handle unload in scheduler
		if retryUnload, err := r.Scheduler.UnloadModel(ctx, model, nil); err != nil {
			if retryUnload {
				return true, ctrl.Result{}, err
			} else {
				model.ObjectMeta.Finalizers = utils.RemoveStr(model.ObjectMeta.Finalizers, constants.ModelFinalizerName)
				if errUpdate := r.Update(ctx, model); errUpdate != nil {
					logger.Error(err, "Failed to remove finalizer", "model", model.Name)
					return true, ctrl.Result{}, err
				}
			}
		}
	}
	// Stop reconciliation as the item is being deleted
	return true, ctrl.Result{}, nil
}

//+kubebuilder:rbac:groups=mlops.seldon.io,resources=models,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=mlops.seldon.io,resources=models/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=mlops.seldon.io,resources=models/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *ModelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("ModelReconcile")
	ctx, cancel := context.WithTimeout(ctx, constants.ReconcileTimeout)
	defer cancel()

	now := time.Now()
	defer func() {
		logger.Info("Finished Model Reconcile", "duration", time.Since(now))
	}()

	model := &mlopsv1alpha1.Model{}
	if err := r.Get(ctx, req.NamespacedName, model); err != nil {
		if errors.IsNotFound(err) {
			// we'll ignore not-found errors, since they can't be fixed by an immediate
			// requeue (we'll need to wait for a new notification), and we can get them
			// on deleted requests.
			return reconcile.Result{}, nil
		}
		logger.Error(err, "unable to fetch Model")
		return reconcile.Result{}, err
	}

	stop, result, err := r.handleFinalizer(ctx, logger, model)
	if stop {
		return result, err
	}

	retry, err := r.Scheduler.LoadModel(ctx, model, nil)
	if err != nil {
		r.updateStatusFromError(ctx, logger, model, retry, err)
		if retry {
			return ctrl.Result{}, err
		} else {
			return ctrl.Result{}, nil
		}
	}

	return ctrl.Result{}, nil
}

func (r *ModelReconciler) updateStatusFromError(
	ctx context.Context,
	logger logr.Logger,
	model *mlopsv1alpha1.Model,
	canRetry bool,
	err error,
) {
	modelStatus := schedulerAPI.ModelStatus_ModelFailed.String()
	if canRetry {
		modelStatus = schedulerAPI.ModelStatus_ModelProgressing.String()
	}

	model.Status.CreateAndSetCondition(mlopsv1alpha1.ModelReady, false, modelStatus, err.Error())
	if errSet := r.Status().Update(ctx, model); errSet != nil {
		logger.Error(errSet, "Failed to set status for model on error", "model", model.Name, "error", err.Error())
	}
}

// SetupWithManager sets up the controller with the Manager.
// Uses https://github.com/kubernetes-sigs/kubebuilder/issues/618#issuecomment-698018831
// This ensures we don't reconcile when just the status is updated by checking if generation changed
func (r *ModelReconciler) SetupWithManager(mgr ctrl.Manager) error {
	pred := predicate.GenerationChangedPredicate{}
	return ctrl.NewControllerManagedBy(mgr).
		For(&mlopsv1alpha1.Model{}).
		WithEventFilter(pred).
		Complete(r)
}
