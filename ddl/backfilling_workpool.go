// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/atomic"
)

// backfillWorkerPool is used to new worker.
type backfillWorkerPool struct {
	exit    atomic.Bool
	resPool *pools.ResourcePool
}

func newBackfillWorkerPool(resPool *pools.ResourcePool) *backfillWorkerPool {
	return &backfillWorkerPool{
		exit:    *atomic.NewBool(false),
		resPool: resPool,
	}
}

// get backfillWorkerPool from context resource pool.
// Please remember to call put after you finished using backfillWorkerPool.
func (wp *backfillWorkerPool) get(sessCtx sessionctx.Context, id int, t table.PhysicalTable, reorgInfo *reorgInfo) (*backfillWorker, error) {
	if wp.resPool == nil {
		return nil, nil
	}

	if wp.exit.Load() {
		return nil, errors.Errorf("backfillWorkerPool is closed")
	}

	// no need to protect wp.resPool
	resource, err := wp.resPool.TryGet()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if resource == nil {
		return nil, nil
	}

	worker := resource.(*backfillWorker)

	worker.id = id
	worker.table = t
	worker.sessCtx = sessCtx
	worker.reorgInfo = reorgInfo
	worker.batchCnt = int(variable.GetDDLReorgBatchSize())
	worker.priority = reorgInfo.Job.Priority

	return worker, nil
}

// put returns workerPool to context resource pool.
func (wp *backfillWorkerPool) put(wk *backfillWorker) {
	if wp.resPool == nil {
		return
	}

	// no need to protect wp.resPool, even the wp.resPool is closed, the ctx still need to
	// put into resPool, because when resPool is closing, it will wait all the ctx returns, then resPool finish closing.
	wp.resPool.Put(wk)
}

// close clean up the workerPool.
func (wp *backfillWorkerPool) close() {
	// prevent closing resPool twice.
	if wp.exit.Load() || wp.resPool == nil {
		return
	}
	wp.exit.Store(true)
	logutil.BgLogger().Info("[ddl] closing workerPool")
	wp.resPool.Close()
}
