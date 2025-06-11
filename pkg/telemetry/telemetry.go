// Copyright 2025 PingCAP, Inc.
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

package telemetry

import (
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// TelemetryInfo records some telemetry information during execution.
type TelemetryInfo struct {
	// DDL related
	MultiSchemaChangeTelemetry *MultiSchemaChangeTelemetryInfo `json:"multi_schema_change,omitempty"`
	UseFlashbackToCluster      *bool                           `json:"flashback_to_cluster,omitempty"`
	CreatePartitionTelemetry   *CreatePartitionTelemetryInfo   `json:"create_partition_telemetry,omitempty"`
	AlterPartitionTelemetry    *AlterPartitionTelemetryInfo    `json:"alter_partition_telemetry,omitempty"`
	AccountLockTelemetry       *AccountLockTelemetryInfo       `json:"account_lock_telemetry,omitempty"`
}

// MultiSchemaChangeTelemetryInfo records multiple schema change information during execution.
type MultiSchemaChangeTelemetryInfo struct {
	SubJobCnt int `json:"sub_job_cnt"`
}

// CreatePartitionTelemetryInfo records create table partition telemetry information during execution.
type CreatePartitionTelemetryInfo struct {
	CreatePartitionType         string `json:"create_partition_type"`
	TablePartitionColumnsCnt    int    `json:"partition_columns_cnt"`
	TablePartitionPartitionsNum uint64 `json:"partitions_cnt"`
	UseCreateIntervalPartition  bool   `json:"use_create_interval_syntax"`
	GlobalIndexCnt              int    `json:"global_index_cnt"`
}

// AlterPartitionTelemetryInfo records alter table partition telemetry information during execution.
type AlterPartitionTelemetryInfo struct {
	UseAddIntervalPartition  bool `json:"use_add_interval_syntax"`
	UseDropIntervalPartition bool `json:"use_drop_interval_syntax"`
	UseReorganizePartition   bool `json:"use_reorg_partition"`
	UseExchangePartition     bool `json:"use_exchange_partition"`
	AddGlobalIndexCnt        int  `json:"add_global_index_cnt"`
}

// AccountLockTelemetryInfo records account lock/unlock information during execution
type AccountLockTelemetryInfo struct {
	// The number of CREATE/ALTER USER statements that lock the user
	LockUser int64 `json:"lock_user_cnt"`
	// The number of CREATE/ALTER USER statements that unlock the user
	UnlockUser int64 `json:"unlock_user_cnt"`
}

// Logger with category "telemetry" is used to log telemetry related messages.
// O11y will try to collect and analyze those logs.
func Logger() *zap.Logger {
	return logutil.BgLogger().With(zap.String(logutil.LogFieldCategory, "telemetry"))
}
