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

package simulation

import "github.com/cockroachdb/cockroach/proto"

type node struct {
	desc   proto.NodeDescriptor
	stores []store
}

func newNode(nodeID proto.NodeID) node {
	node := node{
		desc: proto.NodeDescriptor{
			NodeID: nodeID,
		},
	}
	node.addNewStore()
	return node
}

func (n node) getNextStoreID() proto.StoreID {
	return proto.StoreID((int(n.desc.NodeID) * 100) + len(n.stores))
}

func (n node) addNewStore() {
	n.stores = append(n.stores, newStore(n.getNextStoreID(), n.desc))
}
