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
	stderrors "errors"
	"testing"

	"github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mlopsv1alpha1 "github.com/seldonio/seldon-core/operator/v2/apis/mlops/v1alpha1"
	mock2 "github.com/seldonio/seldon-core/operator/v2/controllers/mlops/mock"
	"github.com/seldonio/seldon-core/operator/v2/pkg/constants"
	testing2 "github.com/seldonio/seldon-core/operator/v2/pkg/utils/testing"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	gomega.NewWithT(t).Expect(mlopsv1alpha1.AddToScheme(scheme)).To(gomega.Succeed())
	return scheme
}

func modelWithStatus(name string, availableReplicas int32, ready bool) *mlopsv1alpha1.Model {
	model := &mlopsv1alpha1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  TestNamespace,
			Finalizers: []string{constants.ModelFinalizerName},
		},
		Spec: mlopsv1alpha1.ModelSpec{
			InferenceArtifactSpec: mlopsv1alpha1.InferenceArtifactSpec{
				StorageURI: "gs://bucket/model",
			},
		},
	}
	model.Status.AvailableReplicas = availableReplicas
	if ready {
		model.Status.CreateAndSetCondition(mlopsv1alpha1.ModelReady, true, "ModelAvailable", "")
	} else {
		model.Status.CreateAndSetCondition(mlopsv1alpha1.ModelReady, false, "ModelProgressing", "rolling out")
	}
	return model
}

func TestHasInProgressRollout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		model *mlopsv1alpha1.Model
		want  bool
	}{
		{
			name:  "no condition means unknown state - no rollout assumed",
			model: &mlopsv1alpha1.Model{},
			want:  false,
		},
		{
			name:  "ready model with replicas is not a rollout",
			model: modelWithStatus("ready", 1, true),
			want:  false,
		},
		{
			name:  "not ready without available replicas is not a rollout",
			model: modelWithStatus("progressing", 0, false),
			want:  false,
		},
		{
			name:  "not ready with available replicas is an in-progress rollout",
			model: modelWithStatus("rolling", 2, false),
			want:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			g.Expect(hasInProgressRollout(tt.model)).To(gomega.Equal(tt.want))
		})
	}
}

func TestModelReconcileDeletion(t *testing.T) {
	t.Parallel()

	now := metav1.Now()

	tests := []struct {
		name            string
		model           *mlopsv1alpha1.Model
		expectUnload    bool
		expectRequeue   bool
		expectFinalizer bool
		expectErr       bool
	}{
		{
			name: "deletion during rollout defers unload and keeps finalizer",
			model: func() *mlopsv1alpha1.Model {
				m := modelWithStatus("rolling-model", 1, false)
				m.ObjectMeta.DeletionTimestamp = &now
				return m
			}(),
			expectUnload:    false,
			expectRequeue:   true,
			expectFinalizer: true,
		},
		{
			name: "deletion with no available replicas unloads immediately",
			model: func() *mlopsv1alpha1.Model {
				m := modelWithStatus("unready-model", 0, false)
				m.ObjectMeta.DeletionTimestamp = &now
				return m
			}(),
			expectUnload:    true,
			expectRequeue:   false,
			expectFinalizer: true, // removed later by the scheduler Terminated event stream
		},
		{
			name: "deletion of ready model unloads immediately",
			model: func() *mlopsv1alpha1.Model {
				m := modelWithStatus("ready-model", 1, true)
				m.ObjectMeta.DeletionTimestamp = &now
				return m
			}(),
			expectUnload:    true,
			expectRequeue:   false,
			expectFinalizer: true, // removed later by the scheduler Terminated event stream
		},
		{
			name: "non-retryable unload error removes finalizer directly",
			model: func() *mlopsv1alpha1.Model {
				m := modelWithStatus("failed-unload-model", 0, false)
				m.ObjectMeta.DeletionTimestamp = &now
				return m
			}(),
			expectUnload:    true,
			expectRequeue:   false,
			expectFinalizer: false,
			expectErr:       false,
		},
		{
			name: "retryable unload error keeps finalizer and returns error",
			model: func() *mlopsv1alpha1.Model {
				m := modelWithStatus("retrying-unload-model", 0, false)
				m.ObjectMeta.DeletionTimestamp = &now
				return m
			}(),
			expectUnload:    true,
			expectRequeue:   false,
			expectFinalizer: true,
			expectErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			ctrlMock := gomock.NewController(t)
			schedulerMock := mock2.NewMockSchedulerClient(ctrlMock)

			fakeClient := testing2.NewFakeClient(newTestScheme(t), tt.model)
			recorder := record.NewFakeRecorder(10)

			reconciler := &ModelReconciler{
				Client:    fakeClient,
				Scheme:    newTestScheme(t),
				Scheduler: schedulerMock,
				Recorder:  recorder,
			}

			if tt.expectUnload {
				switch {
				case tt.expectErr:
					schedulerMock.EXPECT().UnloadModel(gomock.Any(), gomock.Any(), nil).
						Return(true, stderrors.New("scheduler unavailable"))
				case !tt.expectFinalizer:
					// non-retryable error: controller drops the finalizer itself instead of retrying
					schedulerMock.EXPECT().UnloadModel(gomock.Any(), gomock.Any(), nil).
						Return(false, stderrors.New("scheduler rejected unload"))
				default:
					schedulerMock.EXPECT().UnloadModel(gomock.Any(), gomock.Any(), nil).Return(false, nil)
				}
			}

			result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: client.ObjectKey{Name: tt.model.Name, Namespace: TestNamespace},
			})
			if tt.expectErr {
				g.Expect(err).ToNot(gomega.BeNil())
			} else {
				g.Expect(err).To(gomega.BeNil())
			}

			if tt.expectRequeue {
				g.Expect(result.RequeueAfter).To(gomega.BeNumerically(">", 0))
				g.Expect(result.RequeueAfter).To(gomega.BeNumerically("<=", constants.ModelRolloutRequeueInterval))
			} else {
				g.Expect(result).To(gomega.Equal(ctrl.Result{}))
			}

			updated := &mlopsv1alpha1.Model{}
			getErr := fakeClient.Get(context.Background(),
				client.ObjectKey{Name: tt.model.Name, Namespace: TestNamespace}, updated)

			hasFinalizer := false
			if getErr != nil {
				// once the last finalizer is removed from a deleting object the fake client drops
				// the object entirely, which itself proves the finalizer is gone
				g.Expect(apierrors.IsNotFound(getErr)).To(gomega.BeTrue())
			} else {
				for _, f := range updated.ObjectMeta.Finalizers {
					if f == constants.ModelFinalizerName {
						hasFinalizer = true
					}
				}
			}
			g.Expect(hasFinalizer).To(gomega.Equal(tt.expectFinalizer))
		})
	}
}

func TestModelReconcileLoadsNewModel(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)

	ctrlMock := gomock.NewController(t)
	schedulerMock := mock2.NewMockSchedulerClient(ctrlMock)

	model := modelWithStatus("new-model", 0, false)
	fakeClient := testing2.NewFakeClient(newTestScheme(t), model)
	recorder := record.NewFakeRecorder(10)

	reconciler := &ModelReconciler{
		Client:    fakeClient,
		Scheme:    newTestScheme(t),
		Scheduler: schedulerMock,
		Recorder:  recorder,
	}

	// first reconcile adds the finalizer (client update), second one loads the model
	schedulerMock.EXPECT().LoadModel(gomock.Any(), gomock.Any(), nil).Return(false, nil)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Name: model.Name, Namespace: TestNamespace},
	})
	g.Expect(err).To(gomega.BeNil())
	g.Expect(result).To(gomega.Equal(ctrl.Result{}))

	updated := &mlopsv1alpha1.Model{}
	g.Expect(fakeClient.Get(context.Background(),
		client.ObjectKey{Name: model.Name, Namespace: TestNamespace}, updated)).To(gomega.Succeed())
	g.Expect(updated.ObjectMeta.Finalizers).To(gomega.ContainElement(constants.ModelFinalizerName))
}
