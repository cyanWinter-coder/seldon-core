/*
Copyright (c) 2024 Seldon Technologies Ltd.

Use of this software is governed BY
(1) the license included in the LICENSE file or
(2) if the license included in the LICENSE file is the Business Source License 1.1,
the Change License after the Change Date as each is defined in accordance with the LICENSE file.
*/

package store

import (
	"errors"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/seldonio/seldon-core/scheduler/v2/pkg/coordinator"
)

type TestMemoryStore struct {
	*MemoryStore
}

type ModelID struct {
	Name    string
	Version uint32
}

// NewTestMemory DO NOT USE for non-test code. This is purely meant for using in tests where an integration test is
// wanted where the real memory store is needed, but the test needs the ability to directly manipulate the model
// statuses, which can't be achieved with MemoryStore. TestMemoryStore embeds MemoryStore and adds DirectlyUpdateModelStatus
// to modify the statuses.
func NewTestMemory(
	t *testing.T,
	logger log.FieldLogger,
	store *LocalSchedulerStore,
	eventHub *coordinator.EventHub) *TestMemoryStore {
	if t == nil {
		panic("testing.T is required, must only be run via tests")
	}
	m := NewMemoryStore(logger, store, eventHub)
	return &TestMemoryStore{m}
}

func (t *TestMemoryStore) DirectlyUpdateModelStatus(model ModelID, state ModelStatus) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	found, ok := t.store.models[model.Name]
	if !ok {
		return errors.New("model not found")
	}

	version := found.GetVersion(model.Version)
	if version == nil {
		return errors.New("version not found")
	}

	version.state = state
	return nil
}

// DirectlySetReplicaState forces a replica into the given state, bypassing the normal state
// machine. It exists for tests that need to simulate server pod readiness transitions (for
// example an unready pod keeping a new model version in Loading) without driving the agent
// control plane. DO NOT USE for non-test code.
func (t *TestMemoryStore) DirectlySetReplicaState(model ModelID, replicaIdx int, state ModelReplicaState) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	found, ok := t.store.models[model.Name]
	if !ok {
		return errors.New("model not found")
	}

	version := found.GetVersion(model.Version)
	if version == nil {
		return errors.New("version not found")
	}

	version.SetReplicaState(replicaIdx, state, "")
	return nil
}

// DirectlySetServer forces the server assignment of a model version. It exists for tests that
// build scheduling state directly instead of going through the scheduler. DO NOT USE for non-test
// code.
func (t *TestMemoryStore) DirectlySetServer(model ModelID, serverName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	found, ok := t.store.models[model.Name]
	if !ok {
		return errors.New("model not found")
	}

	version := found.GetVersion(model.Version)
	if version == nil {
		return errors.New("version not found")
	}

	version.SetServer(serverName)
	return nil
}
