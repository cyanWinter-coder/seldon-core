/*
Copyright (c) 2024 Seldon Technologies Ltd.

Use of this software is governed by
(1) the license included in the LICENSE file or
(2) if the license included in the LICENSE file is the Business Source License 1.1,
the Change License after the Change Date as each is defined in accordance with the LICENSE file.
*/

package cleaner

import (
	"errors"
	"testing"

	"github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
	"go.uber.org/mock/gomock"

	pb "github.com/seldonio/seldon-core/apis/go/v2/mlops/scheduler"

	"github.com/seldonio/seldon-core/scheduler/v2/pkg/store"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/store/mock"
)

func newCleanerModelVersion(t *testing.T, version uint32, server string, replicas map[int]store.ReplicaStatus, state store.ModelState) *store.ModelVersion {
	t.Helper()
	return store.NewModelVersion(
		&pb.Model{
			Meta:           &pb.MetaData{Name: "foo"},
			ModelSpec:      &pb.ModelSpec{Uri: "gs://bucket/model"},
			DeploymentSpec: &pb.DeploymentSpec{Replicas: 1},
		},
		version,
		server,
		replicas,
		false,
		state,
	)
}

func availableReplicas() map[int]store.ReplicaStatus {
	return map[int]store.ReplicaStatus{0: {State: store.Available}}
}

func loadingReplicas() map[int]store.ReplicaStatus {
	return map[int]store.ReplicaStatus{0: {State: store.Loading}}
}

func TestCleanupOldVersions(t *testing.T) {
	logger := log.New()

	type test struct {
		name string
		// versions in ascending order as held by the store
		versions             []*store.ModelVersion
		modelGwState         store.ModelState
		expectUnloadVersions []uint32
		expectModelGwUnload  []uint32
	}

	tests := []test{
		{
			name: "latest available and old version without live replicas is unloaded",
			versions: []*store.ModelVersion{
				newCleanerModelVersion(t, 1, "server", map[int]store.ReplicaStatus{0: {State: store.Unloaded}}, store.ModelTerminated),
				newCleanerModelVersion(t, 2, "server", availableReplicas(), store.ModelAvailable),
			},
			expectUnloadVersions: []uint32{1},
		},
		{
			name: "old version retained while it still has live replicas during version switch",
			versions: []*store.ModelVersion{
				newCleanerModelVersion(t, 1, "server", availableReplicas(), store.ModelAvailable),
				newCleanerModelVersion(t, 2, "server", loadingReplicas(), store.ModelProgressing),
			},
			expectUnloadVersions: nil,
		},
		{
			name: "latest not available means no old versions are cleaned",
			versions: []*store.ModelVersion{
				newCleanerModelVersion(t, 1, "server", availableReplicas(), store.ModelAvailable),
				newCleanerModelVersion(t, 2, "server", loadingReplicas(), store.ModelProgressing),
			},
			expectUnloadVersions: nil,
		},
		{
			name: "multiple old versions: live one retained, inactive one unloaded",
			versions: []*store.ModelVersion{
				newCleanerModelVersion(t, 1, "server", map[int]store.ReplicaStatus{0: {State: store.Unloaded}}, store.ModelTerminated),
				newCleanerModelVersion(t, 2, "server", availableReplicas(), store.ModelAvailable),
				newCleanerModelVersion(t, 3, "server", availableReplicas(), store.ModelAvailable),
			},
			expectUnloadVersions: []uint32{1},
		},
		{
			name: "model gateway cleanup independent of replica retention",
			versions: []*store.ModelVersion{
				newCleanerModelVersion(t, 1, "server", availableReplicas(), store.ModelAvailable),
				newCleanerModelVersion(t, 2, "server", loadingReplicas(), store.ModelProgressing),
			},
			modelGwState:        store.ModelAvailable,
			expectModelGwUnload: []uint32{1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			ctrl := gomock.NewController(t)
			mockStore := mock.NewMockModelStore(ctrl)

			snapshot := &store.ModelSnapshot{Name: "foo", Versions: tt.versions}
			if tt.modelGwState != 0 {
				// apply the model gateway state onto the latest version for this scenario
				latest := snapshot.GetLatest()
				latest.SetModelState(store.ModelStatus{
					State:        latest.ModelState().State,
					ModelGwState: tt.modelGwState,
				})
			}

			mockStore.EXPECT().LockModel("foo")
			mockStore.EXPECT().UnlockModel("foo")
			mockStore.EXPECT().GetModel("foo").Return(snapshot, nil)
			for _, v := range tt.expectUnloadVersions {
				mockStore.EXPECT().UnloadVersionModels("foo", v).Return(true, nil)
			}
			for _, v := range tt.expectModelGwUnload {
				mockStore.EXPECT().UnloadModelGwVersionModels("foo", v).Return(true, nil)
			}

			cleaner := NewTestVersionCleaner(mockStore, logger)
			err := cleaner.CleanupOldVersions("foo")
			g.Expect(err).To(gomega.BeNil())
		})
	}
}

func TestCleanupOldVersionsPropagatesUnloadError(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	ctrl := gomock.NewController(t)
	mockStore := mock.NewMockModelStore(ctrl)
	logger := log.New()

	snapshot := &store.ModelSnapshot{
		Name: "foo",
		Versions: []*store.ModelVersion{
			newCleanerModelVersion(t, 1, "server", map[int]store.ReplicaStatus{0: {State: store.Unloaded}}, store.ModelTerminated),
			newCleanerModelVersion(t, 2, "server", availableReplicas(), store.ModelAvailable),
		},
	}

	expectedErr := errors.New("unload failed")
	mockStore.EXPECT().LockModel("foo")
	mockStore.EXPECT().UnlockModel("foo")
	mockStore.EXPECT().GetModel("foo").Return(snapshot, nil)
	mockStore.EXPECT().UnloadVersionModels("foo", uint32(1)).Return(false, expectedErr)

	cleaner := NewTestVersionCleaner(mockStore, logger)
	err := cleaner.CleanupOldVersions("foo")
	g.Expect(err).To(gomega.MatchError(expectedErr))
}
