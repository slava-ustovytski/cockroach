// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package storage

import (
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// repairQueueMaxSize is the max size of the replicate queue.
	repairQueueMaxSize = 100

	// replicaQueueTimerDuration is the duration between removals of queued
	// replicas.
	repairQueueTimerDuration = 0 // zero duration to process removals greedily
)

// repairQueue manages a queue of replicas which may have one of its other
// replica on a dead store.
type repairQueue struct {
	*baseQueue
	storePool      *StorePool
	replicateQueue *replicateQueue
	clock          *hlc.Clock
}

// makeRepairQueue returns a new instance of repairQueue.
func makeRepairQueue(storePool *StorePool, replicateQueue *replicateQueue, clock *hlc.Clock) repairQueue {
	rq := repairQueue{
		storePool:      storePool,
		replicateQueue: replicateQueue,
		clock:          clock,
	}
	// rq must be a pointer in order to setup the reference cycle.
	rq.baseQueue = newBaseQueue("repair", &rq, repairQueueMaxSize)
	return rq
}

// needsLeaderLease implements the queueImpl interface.
// This should not require a leader lease, but if we find that there is too
// much contention on the replica removals we can revisit that decision.
func (rq repairQueue) needsLeaderLease() bool {
	return false
}

// shouldQueue implements the queueImpl interface.
func (rq repairQueue) shouldQueue(now proto.Timestamp, repl *Replica) (shouldQ bool, priority float64) {
	shouldQ, priority, _ = rq.needsRepair(repl)
	return
}

// needsRepair determines if any replicas that belong to the same range as repl
// belong to a dead store. If so, it returns the priority (the number of
// replicas on dead stores) and the list of these 'dead' replicas.
func (rq repairQueue) needsRepair(repl *Replica) (bool, float64, []proto.Replica) {
	rangeDesc := repl.Desc()
	var deadReplicas []proto.Replica
	for _, otherRepl := range rangeDesc.Replicas {
		storeDetail := rq.storePool.getStoreDetail(otherRepl.StoreID)
		if storeDetail == nil || storeDetail.dead {
			deadReplicas = append(deadReplicas, otherRepl)
		}
	}

	if len(deadReplicas) == 0 {
		return false, float64(0), deadReplicas
	}

	// Does the range still have a quorum of live replicas? We shouldn't remove
	// one without at least a quorum.
	quorum := (len(rangeDesc.Replicas) / 2) + 1
	liveReplicas := len(rangeDesc.Replicas) - len(deadReplicas)
	return liveReplicas >= quorum, float64(len(deadReplicas)), deadReplicas
}

// process implements the queueImpl interface.
// If the replica still requires repairing, it removes the replicas on dead
// stores (one at a time) and then queues up the replica for replication on the
// repliateQueue.
func (rq repairQueue) process(now proto.Timestamp, repl *Replica) error {
	shouldQ, _, deadReplicas := rq.needsRepair(repl)
	if !shouldQ {
		return nil
	}

	for _, deadReplica := range deadReplicas {
		log.Warningf("The replica %v is being removed from its dead store.", deadReplica)
		log.Errorf("The replica %v would be queue to remove here.", deadReplica)
		//		TODO(bram): once remove replica is ready, uncomment this.
		//		if err := repl.ChangeReplicas(proto.REMOVE_REPLICA, deadReplica, repl.Desc()); err != nil {
		//			return err
		//		}

		// Enqueue the replica in the replicate queue so the new replica can be
		// added.
		rq.replicateQueue.MaybeAdd(repl, rq.clock.Now())
	}

	return nil
}

// timer implements the queueImpl interface.
func (rq repairQueue) timer() time.Duration {
	return repairQueueTimerDuration
}
