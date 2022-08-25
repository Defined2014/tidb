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
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/gcutil"
)

var pdScheduleKey = []string{
	"hot-region-schedule-limit",
	"leader-schedule-limit",
	"merge-schedule-limit",
	"region-schedule-limit",
	"replica-schedule-limit"}

func closePDSchedule(job *model.Job) error {
	if err := savePDSchedule(job); err != nil {
		return err
	}
	saveValue := make(map[string]interface{})
	for _, key := range pdScheduleKey {
		saveValue[key] = 0
	}
	return infosync.SetPDScheduleConfig(context.Background(), saveValue)
}

func savePDSchedule(job *model.Job) error {
	retValue, err := infosync.GetPDScheduleConfig(context.Background())
	if err != nil {
		return err
	}
	saveValue := make(map[string]interface{})
	for _, key := range pdScheduleKey {
		saveValue[key] = retValue[key]
	}
	job.Args = append(job.Args, saveValue)
	return nil
}

func recoverPDSchedule(pdScheduleParam map[string]interface{}) error {
	if pdScheduleParam == nil {
		return nil
	}
	return infosync.SetPDScheduleConfig(context.Background(), pdScheduleParam)
}

func checkFlashbackCluster(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job, flashbackTS uint64, flashbackJobID int64) error {
	nowSchemaVersion, err := t.GetSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	flashbackSchemaVersion, err := meta.NewSnapshotMeta(d.store.GetSnapshot(kv.NewVersion(flashbackTS))).GetSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	// If flashbackSchemaVersion not same as nowSchemaVersion, we've done ddl during [flashbackTs, now).
	if flashbackSchemaVersion != nowSchemaVersion {
		return errors.Errorf("schema version not same, have done ddl during [flashbackTS, now)")
	}

	if flashbackJobID == 0 {
		sess, err := w.sessPool.get()
		if err != nil {
			return errors.Trace(err)
		}
		defer w.sessPool.put(sess)

		jobs, err := GetAllDDLJobs(sess, t)
		if err != nil {
			return errors.Trace(err)
		}
		// Other non-flashback ddl jobs in queue, return error.
		if len(jobs) != 1 {
			var otherJob *model.Job
			for _, j := range jobs {
				if j.ID != job.ID {
					otherJob = j
					break
				}
			}
			return errors.Errorf("have other ddl jobs(jobID: %d) in queue, can't do flashback", otherJob.ID)
		}
		if err = gcutil.DisableGC(sess); err != nil {
			return err
		}
		if err = closePDSchedule(job); err != nil {
			return err
		}
		return kv.RunInNewTxn(w.ctx, w.store, true, func(ctx context.Context, txn kv.Transaction) error {
			return meta.NewMeta(txn).SetFlashbackClusterJobID(job.ID)
		})
	}

	return nil
}

func (w *worker) onFlashbackCluster(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	var flashbackTS uint64
	var minTableID int64
	if err := job.DecodeArgs(&flashbackTS, &minTableID); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	flashbackJobID, err := t.GetFlashbackClusterJobID()
	if err != nil {
		return ver, err
	}

	if err = checkFlashbackCluster(w, d, t, job, flashbackTS, flashbackJobID); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if flashbackJobID == 0 {
		startKey := tablecodec.EncodeTablePrefix(minTableID)
		endKey := tablecodec.EncodeTablePrefix(meta.MaxGlobalID)

		kvRanges, err := splitKeyRanges(d.store, startKey, endKey)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	job.State = model.JobStateDone
	return ver, errors.Trace(err)
}

func finishFlashbackCluster(w *worker, job *model.Job) error {
	var flashbackTS, maxTableID uint64
	var pdScheduleValue map[string]interface{}
	if err := job.DecodeArgs(&flashbackTS, &maxTableID, &pdScheduleValue); err != nil {
		return errors.Trace(err)
	}

	err := kv.RunInNewTxn(w.ctx, w.store, true, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		jobID, err := t.GetFlashbackClusterJobID()
		if err != nil {
			return err
		}
		if jobID == job.ID {
			if err = gcutil.EnableGC(w.sess.session()); err != nil {
				return err
			}
			if err = recoverPDSchedule(pdScheduleValue); err != nil {
				return err
			}
			return meta.NewMeta(txn).SetFlashbackClusterJobID(0)
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
