// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// 拿到所有的store的信息
	stores := make([]*core.StoreInfo, 0)
	for _, s := range cluster.GetStores() {
		if s.IsUp() && s.DownTime() <= cluster.GetMaxStoreDownTime() {
			stores = append(stores, s)
		}
	}
	// 然后根据它们的 region 大小进行排序，总的的size大小
	for i := 0; i < len(stores)-1; i++ {
		for j := 0; j < len(stores)-i-1; j++ {
			if stores[j].GetRegionSize() < stores[j+1].GetRegionSize() {
				stores[j], stores[j+1] = stores[j+1], stores[j]
			}
		}
	}
	// 如果只有一个不超过一个store，则不需要调度
	if len(stores) <= 1 {
		return nil
	}
	// 找到最适合在 store 中移动的 region
	var region *core.RegionInfo
	var toSchedule *core.StoreInfo
	for i, s := range stores {
		toSchedule = s
		if i == len(stores)-1 {
			break
		}
		// 首先尝试选择一个挂起的 region
		cluster.GetPendingRegionsWithLock(s.GetID(), func(container core.RegionsContainer) {
			region = container.RandomRegion(nil, nil)
		})
		if region != nil {
			break
		}
		// 如果没有一个挂起的 region，尝试找到一个 Follower region
		cluster.GetFollowersWithLock(s.GetID(), func(container core.RegionsContainer) {
			region = container.RandomRegion(nil, nil)
		})
		if region != nil {
			break
		}
		// 如果仍然不能挑选出一个 region，尝试挑选领导 region
		cluster.GetLeadersWithLock(s.GetID(), func(container core.RegionsContainer) {
			region = container.RandomRegion(nil, nil)
		})
		if region != nil {
			break
		}
	}
	// 没有选出，则返回
	if region == nil {
		return nil
	}
	// 有掉线的store,这个时候不应该调度进行balance,必须在齐全的情况下调度
	if len(region.GetStoreIds()) < cluster.GetMaxReplicas() {
		return nil
	}
	// 选择 region 大小最小的 store 最为调入目标
	// 通过检查原始 store 和目标 store 的 region 大小之间的差异来判断这种移动是否有价值

	// 首先找到这个region不在哪些store上
	// 拿到region存在的store的信息
	regionStores := cluster.GetRegionStores(region)
	toStores := make([]*core.StoreInfo, 0)
	for _, s := range stores {
		// s 是否在 region所在的 stores裏面
		isIn := false
		for _, store := range regionStores {
			if store.GetID() == s.GetID() {
				isIn = true
				break
			}
		}
		if !isIn {
			toStores = append(toStores, s)
		}
	}
	if len(toStores) == 0 {
		return nil
	}
	// 从小到达排序不在region的store
	for i := 0; i < len(toStores)-1; i++ {
		for j := 0; j < len(toStores)-i-1; j++ {
			if toStores[j].GetRegionSize() > toStores[j+1].GetRegionSize() {
				toStores[j], toStores[j+1] = toStores[j+1], toStores[j]
			}
		}
	}
	taget := toStores[0]
	// 确保这个差值必须大于 region 近似大小的2倍,不然变化之后，大小还是差距没有缩小很多，而且大小关系变化了
	if toSchedule.GetRegionSize()-taget.GetRegionSize() <= 2*region.GetApproximateSize() {
		return nil
	}
	// 在目标 store 上分配一个新的 peer 并创建一个移动 peer 操作
	peer, err := cluster.AllocPeer(taget.GetID())
	if err != nil {
		fmt.Println("err : ", err)
		return nil
	}
	// 进行movepeer的操作
	peerOperator, err := operator.CreateMovePeerOperator("balance", cluster, region, operator.OpBalance, toSchedule.GetID(), taget.GetID(), peer.GetId())
	if err != nil {
		return nil
	}
	return peerOperator
}
