/*
Copyright (c) 2024 Seldon Technologies Ltd.

Use of this software is governed by
(1) the license included in the LICENSE file or
(2) if the license included in the LICENSE file is the Business Source License 1.1,
the Change License after the Change Date as each is defined in accordance with the LICENSE file.
*/

package cleaner

import (
	"testing"

	"github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"

	pb "github.com/seldonio/seldon-core/apis/go/v2/mlops/scheduler"

	"github.com/seldonio/seldon-core/scheduler/v2/pkg/coordinator"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/store"
)

// TestCleanupOldVersionsRolloutScenario drives the real memory store through the scenario where a
// model version switch, an unready server pod and a configuration update arrive together:
//
//  1. version 1 is loaded and available (old version still serving),
//  2. version 2 is created (model spec change) and scheduled onto a server pod,
//  3. the pod is not ready yet so version 2 stays progressing,
//  4. cleanup must not touch version 1,
//  5. once version 2 becomes available and version 1 loses its live replicas, a later cleanup
//     pass unloads version 1.
func TestCleanupOldVersionsRolloutScenario(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	logger := log.New()

	eventHub, err := coordinator.NewEventHub(logger)
	g.Expect(err).To(gomega.BeNil())
	modelStore := store.NewTestMemory(t, logger, store.NewLocalSchedulerStore(), eventHub)

	const modelName = "rollout-model"
	loadReq := func(generation int64, uri string, replicas uint32) *pb.LoadModelRequest {
		return &pb.LoadModelRequest{
			Model: &pb.Model{
				Meta: &pb.MetaData{
					Name: modelName,
					KubernetesMeta: &pb.KubernetesMeta{
						Generation: generation,
					},
				},
				ModelSpec: &pb.ModelSpec{
					Uri: uri,
				},
				DeploymentSpec: &pb.DeploymentSpec{
					Replicas: replicas,
				},
			},
		}
	}

	v1ID := store.ModelID{Name: modelName, Version: 1}
	v2ID := store.ModelID{Name: modelName, Version: 2}

	// version 1: initial model, loaded and available
	g.Expect(modelStore.UpdateModel(loadReq(1, "gs://bucket/v1", 1))).To(gomega.BeNil())
	model, err := modelStore.GetModel(modelName)
	g.Expect(err).To(gomega.BeNil())
	g.Expect(model.GetLatest().GetVersion()).To(gomega.Equal(uint32(1)))
	g.Expect(modelStore.DirectlySetServer(v1ID, "server")).To(gomega.BeNil())
	g.Expect(modelStore.DirectlySetReplicaState(v1ID, 0, store.Available)).To(gomega.BeNil())
	g.Expect(modelStore.DirectlyUpdateModelStatus(v1ID,
		store.ModelStatus{State: store.ModelAvailable})).To(gomega.BeNil())

	// version 2: model spec change creates a new version while version 1 keeps serving
	g.Expect(modelStore.UpdateModel(loadReq(2, "gs://bucket/v2", 1))).To(gomega.BeNil())
	model, err = modelStore.GetModel(modelName)
	g.Expect(err).To(gomega.BeNil())
	g.Expect(model.Versions).To(gomega.HaveLen(2))
	g.Expect(model.GetLatest().GetVersion()).To(gomega.Equal(uint32(2)))

	// pod is not ready: version 2 only reaches Loading, so the latest version stays progressing
	g.Expect(modelStore.DirectlySetServer(v2ID, "server")).To(gomega.BeNil())
	g.Expect(modelStore.DirectlySetReplicaState(v2ID, 0, store.Loading)).To(gomega.BeNil())
	g.Expect(modelStore.DirectlyUpdateModelStatus(v2ID,
		store.ModelStatus{State: store.ModelProgressing})).To(gomega.BeNil())

	cleaner := NewTestVersionCleaner(modelStore, logger)

	// first cleanup: latest not available and old version still live - nothing must be unloaded
	g.Expect(cleaner.CleanupOldVersions(modelName)).To(gomega.BeNil())
	model, err = modelStore.GetModel(modelName)
	g.Expect(err).To(gomega.BeNil())
	g.Expect(model.GetPrevious().GetModelReplicaState(0)).To(gomega.Equal(store.Available))
	g.Expect(model.GetLatest().GetModelReplicaState(0)).To(gomega.Equal(store.Loading))

	// new pod becomes ready: version 2 reaches Available
	g.Expect(modelStore.DirectlyUpdateModelStatus(v2ID,
		store.ModelStatus{State: store.ModelAvailable})).To(gomega.BeNil())

	// even with the latest available, version 1 still holds live replicas and must be retained
	g.Expect(cleaner.CleanupOldVersions(modelName)).To(gomega.BeNil())
	model, err = modelStore.GetModel(modelName)
	g.Expect(err).To(gomega.BeNil())
	g.Expect(model.GetPrevious().GetModelReplicaState(0)).To(gomega.Equal(store.Available))

	// old version's replica is finally gone (scheduler moved it to Unloaded) - now cleanup unloads it
	g.Expect(modelStore.DirectlySetReplicaState(v1ID, 0, store.Unloaded)).To(gomega.BeNil())
	g.Expect(modelStore.DirectlyUpdateModelStatus(v1ID,
		store.ModelStatus{State: store.ModelTerminated})).To(gomega.BeNil())

	// cleanup now unloads the inactive old version; a second pass is a no-op
	g.Expect(cleaner.CleanupOldVersions(modelName)).To(gomega.BeNil())
	unloaded, err := modelStore.UnloadVersionModels(modelName, 1)
	g.Expect(err).To(gomega.BeNil())
	g.Expect(unloaded).To(gomega.BeFalse())
}

// TestCleanupOldVersionsConfigUpdateKeepsPreviousAvailable verifies that a deployment-only change
// (configuration update that does not create a new scheduler version) does not make an available
// model eligible for cleanup, while a following model-spec rollout still protects the old version.
func TestCleanupOldVersionsConfigUpdateKeepsPreviousAvailable(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	logger := log.New()

	eventHub, err := coordinator.NewEventHub(logger)
	g.Expect(err).To(gomega.BeNil())
	modelStore := store.NewTestMemory(t, logger, store.NewLocalSchedulerStore(), eventHub)

	const modelName = "config-update-model"
	req := &pb.LoadModelRequest{
		Model: &pb.Model{
			Meta:           &pb.MetaData{Name: modelName, KubernetesMeta: &pb.KubernetesMeta{Generation: 1}},
			ModelSpec:      &pb.ModelSpec{Uri: "gs://bucket/model"},
			DeploymentSpec: &pb.DeploymentSpec{Replicas: 1},
		},
	}
	g.Expect(modelStore.UpdateModel(req)).To(gomega.BeNil())

	// deployment-only spec change: same version is updated in place, no extra version created
	req.Model.DeploymentSpec.Replicas = 2
	g.Expect(modelStore.UpdateModel(req)).To(gomega.BeNil())
	model, err := modelStore.GetModel(modelName)
	g.Expect(err).To(gomega.BeNil())
	g.Expect(model.Versions).To(gomega.HaveLen(1))

	cleaner := NewTestVersionCleaner(modelStore, logger)
	g.Expect(cleaner.CleanupOldVersions(modelName)).To(gomega.BeNil())

	// now a model-spec rollout adds a second version; the first stays live and must be retained
	req2 := &pb.LoadModelRequest{
		Model: &pb.Model{
			Meta:           &pb.MetaData{Name: modelName, KubernetesMeta: &pb.KubernetesMeta{Generation: 2}},
			ModelSpec:      &pb.ModelSpec{Uri: "gs://bucket/model-v2"},
			DeploymentSpec: &pb.DeploymentSpec{Replicas: 2},
		},
	}
	g.Expect(modelStore.UpdateModel(req2)).To(gomega.BeNil())
	model, err = modelStore.GetModel(modelName)
	g.Expect(err).To(gomega.BeNil())
	g.Expect(model.Versions).To(gomega.HaveLen(2))

	v1ID := store.ModelID{Name: modelName, Version: model.Versions[0].GetVersion()}
	v2ID := store.ModelID{Name: modelName, Version: model.Versions[1].GetVersion()}

	g.Expect(modelStore.DirectlySetServer(v1ID, "server")).To(gomega.BeNil())
	g.Expect(modelStore.DirectlySetReplicaState(v1ID, 0, store.Available)).To(gomega.BeNil())
	g.Expect(modelStore.DirectlyUpdateModelStatus(v1ID,
		store.ModelStatus{State: store.ModelAvailable})).To(gomega.BeNil())
	g.Expect(modelStore.DirectlySetServer(v2ID, "server")).To(gomega.BeNil())
	g.Expect(modelStore.DirectlySetReplicaState(v2ID, 0, store.Loading)).To(gomega.BeNil())
	g.Expect(modelStore.DirectlyUpdateModelStatus(v2ID,
		store.ModelStatus{State: store.ModelProgressing})).To(gomega.BeNil())

	g.Expect(cleaner.CleanupOldVersions(modelName)).To(gomega.BeNil())
	model, err = modelStore.GetModel(modelName)
	g.Expect(err).To(gomega.BeNil())
	g.Expect(model.Versions[0].GetModelReplicaState(0)).To(gomega.Equal(store.Available))
}
