// Copyright 2024 PingCAP, Inc.
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

package logsplit_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/docker/go-units"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	logsplit "github.com/pingcap/tidb/br/pkg/restore/internal/log_split"
	snapsplit "github.com/pingcap/tidb/br/pkg/restore/internal/snap_split"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/utiltest"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func keyWithTablePrefix(tableID int64, key string) []byte {
	rawKey := append(tablecodec.GenTableRecordPrefix(tableID), []byte(key)...)
	return codec.EncodeBytes([]byte{}, rawKey)
}

func TestSplitPoint(t *testing.T) {
	ctx := context.Background()
	var oldTableID int64 = 50
	var tableID int64 = 100
	rewriteRules := &restoreutils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTableID),
				NewKeyPrefix: tablecodec.EncodeTablePrefix(tableID),
			},
		},
	}

	// range:     b   c d   e       g         i
	//            +---+ +---+       +---------+
	//          +-------------+----------+---------+
	// region:  a             f          h         j
	splitHelper := logsplit.NewSplitHelper()
	splitHelper.Merge(logsplit.Valued{Key: logsplit.Span{StartKey: keyWithTablePrefix(oldTableID, "b"), EndKey: keyWithTablePrefix(oldTableID, "c")}, Value: logsplit.Value{Size: 100, Number: 100}})
	splitHelper.Merge(logsplit.Valued{Key: logsplit.Span{StartKey: keyWithTablePrefix(oldTableID, "d"), EndKey: keyWithTablePrefix(oldTableID, "e")}, Value: logsplit.Value{Size: 200, Number: 200}})
	splitHelper.Merge(logsplit.Valued{Key: logsplit.Span{StartKey: keyWithTablePrefix(oldTableID, "g"), EndKey: keyWithTablePrefix(oldTableID, "i")}, Value: logsplit.Value{Size: 300, Number: 300}})
	client := utiltest.NewFakeSplitClient()
	client.AppendRegion(keyWithTablePrefix(tableID, "a"), keyWithTablePrefix(tableID, "f"))
	client.AppendRegion(keyWithTablePrefix(tableID, "f"), keyWithTablePrefix(tableID, "h"))
	client.AppendRegion(keyWithTablePrefix(tableID, "h"), keyWithTablePrefix(tableID, "j"))
	client.AppendRegion(keyWithTablePrefix(tableID, "j"), keyWithTablePrefix(tableID+1, "a"))

	iter := logsplit.NewSplitHelperIteratorForTest(splitHelper, tableID, rewriteRules)
	err := logsplit.SplitPoint(ctx, iter, client, func(ctx context.Context, rs *snapsplit.RegionSplitter, u uint64, o int64, ri *split.RegionInfo, v []logsplit.Valued) error {
		require.Equal(t, u, uint64(0))
		require.Equal(t, o, int64(0))
		require.Equal(t, ri.Region.StartKey, keyWithTablePrefix(tableID, "a"))
		require.Equal(t, ri.Region.EndKey, keyWithTablePrefix(tableID, "f"))
		require.EqualValues(t, v[0].Key.StartKey, keyWithTablePrefix(tableID, "b"))
		require.EqualValues(t, v[0].Key.EndKey, keyWithTablePrefix(tableID, "c"))
		require.EqualValues(t, v[1].Key.StartKey, keyWithTablePrefix(tableID, "d"))
		require.EqualValues(t, v[1].Key.EndKey, keyWithTablePrefix(tableID, "e"))
		require.Equal(t, len(v), 2)
		return nil
	})
	require.NoError(t, err)
}

func getCharFromNumber(prefix string, i int) string {
	c := '1' + (i % 10)
	b := '1' + (i%100)/10
	a := '1' + i/100
	return fmt.Sprintf("%s%c%c%c", prefix, a, b, c)
}

func TestSplitPoint2(t *testing.T) {
	ctx := context.Background()
	var oldTableID int64 = 50
	var tableID int64 = 100
	rewriteRules := &restoreutils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTableID),
				NewKeyPrefix: tablecodec.EncodeTablePrefix(tableID),
			},
		},
	}

	// range:     b   c d   e f                 i j    k l        n
	//            +---+ +---+ +-----------------+ +----+ +--------+
	//          +---------------+--+.....+----+------------+---------+
	// region:  a               g   >128      h            m         o
	splitHelper := logsplit.NewSplitHelper()
	splitHelper.Merge(logsplit.Valued{Key: logsplit.Span{StartKey: keyWithTablePrefix(oldTableID, "b"), EndKey: keyWithTablePrefix(oldTableID, "c")}, Value: logsplit.Value{Size: 100, Number: 100}})
	splitHelper.Merge(logsplit.Valued{Key: logsplit.Span{StartKey: keyWithTablePrefix(oldTableID, "d"), EndKey: keyWithTablePrefix(oldTableID, "e")}, Value: logsplit.Value{Size: 200, Number: 200}})
	splitHelper.Merge(logsplit.Valued{Key: logsplit.Span{StartKey: keyWithTablePrefix(oldTableID, "f"), EndKey: keyWithTablePrefix(oldTableID, "i")}, Value: logsplit.Value{Size: 300, Number: 300}})
	splitHelper.Merge(logsplit.Valued{Key: logsplit.Span{StartKey: keyWithTablePrefix(oldTableID, "j"), EndKey: keyWithTablePrefix(oldTableID, "k")}, Value: logsplit.Value{Size: 200, Number: 200}})
	splitHelper.Merge(logsplit.Valued{Key: logsplit.Span{StartKey: keyWithTablePrefix(oldTableID, "l"), EndKey: keyWithTablePrefix(oldTableID, "n")}, Value: logsplit.Value{Size: 200, Number: 200}})
	client := utiltest.NewFakeSplitClient()
	client.AppendRegion(keyWithTablePrefix(tableID, "a"), keyWithTablePrefix(tableID, "g"))
	client.AppendRegion(keyWithTablePrefix(tableID, "g"), keyWithTablePrefix(tableID, getCharFromNumber("g", 0)))
	for i := 0; i < 256; i++ {
		client.AppendRegion(keyWithTablePrefix(tableID, getCharFromNumber("g", i)), keyWithTablePrefix(tableID, getCharFromNumber("g", i+1)))
	}
	client.AppendRegion(keyWithTablePrefix(tableID, getCharFromNumber("g", 256)), keyWithTablePrefix(tableID, "h"))
	client.AppendRegion(keyWithTablePrefix(tableID, "h"), keyWithTablePrefix(tableID, "m"))
	client.AppendRegion(keyWithTablePrefix(tableID, "m"), keyWithTablePrefix(tableID, "o"))
	client.AppendRegion(keyWithTablePrefix(tableID, "o"), keyWithTablePrefix(tableID+1, "a"))

	firstSplit := true
	iter := logsplit.NewSplitHelperIteratorForTest(splitHelper, tableID, rewriteRules)
	err := logsplit.SplitPoint(ctx, iter, client, func(ctx context.Context, rs *snapsplit.RegionSplitter, u uint64, o int64, ri *split.RegionInfo, v []logsplit.Valued) error {
		if firstSplit {
			require.Equal(t, u, uint64(0))
			require.Equal(t, o, int64(0))
			require.Equal(t, ri.Region.StartKey, keyWithTablePrefix(tableID, "a"))
			require.Equal(t, ri.Region.EndKey, keyWithTablePrefix(tableID, "g"))
			require.EqualValues(t, v[0].Key.StartKey, keyWithTablePrefix(tableID, "b"))
			require.EqualValues(t, v[0].Key.EndKey, keyWithTablePrefix(tableID, "c"))
			require.EqualValues(t, v[1].Key.StartKey, keyWithTablePrefix(tableID, "d"))
			require.EqualValues(t, v[1].Key.EndKey, keyWithTablePrefix(tableID, "e"))
			require.EqualValues(t, v[2].Key.StartKey, keyWithTablePrefix(tableID, "f"))
			require.EqualValues(t, v[2].Key.EndKey, keyWithTablePrefix(tableID, "g"))
			require.Equal(t, v[2].Value.Size, uint64(1))
			require.Equal(t, v[2].Value.Number, int64(1))
			require.Equal(t, len(v), 3)
			firstSplit = false
		} else {
			require.Equal(t, u, uint64(1))
			require.Equal(t, o, int64(1))
			require.Equal(t, ri.Region.StartKey, keyWithTablePrefix(tableID, "h"))
			require.Equal(t, ri.Region.EndKey, keyWithTablePrefix(tableID, "m"))
			require.EqualValues(t, v[0].Key.StartKey, keyWithTablePrefix(tableID, "j"))
			require.EqualValues(t, v[0].Key.EndKey, keyWithTablePrefix(tableID, "k"))
			require.EqualValues(t, v[1].Key.StartKey, keyWithTablePrefix(tableID, "l"))
			require.EqualValues(t, v[1].Key.EndKey, keyWithTablePrefix(tableID, "m"))
			require.Equal(t, v[1].Value.Size, uint64(100))
			require.Equal(t, v[1].Value.Number, int64(100))
			require.Equal(t, len(v), 2)
		}
		return nil
	})
	require.NoError(t, err)
}

func fakeFile(tableID, rowID int64, length uint64, num int64) *backuppb.DataFileInfo {
	return &backuppb.DataFileInfo{
		StartKey:        fakeRowKey(tableID, rowID),
		EndKey:          fakeRowKey(tableID, rowID+1),
		TableId:         tableID,
		Length:          length,
		NumberOfEntries: num,
	}
}

func fakeRowKey(tableID, rowID int64) kv.Key {
	return codec.EncodeBytes(nil, tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(tableID), kv.IntHandle(rowID)))
}

func TestLogSplitHelper(t *testing.T) {
	ctx := context.Background()
	rules := map[int64]*restoreutils.RewriteRules{
		1: {
			Data: []*import_sstpb.RewriteRule{
				{
					OldKeyPrefix: tablecodec.GenTableRecordPrefix(1),
					NewKeyPrefix: tablecodec.GenTableRecordPrefix(100),
				},
			},
		},
		2: {
			Data: []*import_sstpb.RewriteRule{
				{
					OldKeyPrefix: tablecodec.GenTableRecordPrefix(2),
					NewKeyPrefix: tablecodec.GenTableRecordPrefix(200),
				},
			},
		},
	}
	oriRegions := [][]byte{
		{},
		codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(100)),
		codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(200)),
		codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(402)),
	}
	mockPDCli := split.NewMockPDClientForSplit()
	mockPDCli.SetRegions(oriRegions)
	client := split.NewClient(mockPDCli, nil, nil, 100, 4)
	helper := logsplit.NewLogSplitHelper(rules, client, 4*units.MiB, 400)

	helper.Merge(fakeFile(1, 100, 100, 100))
	helper.Merge(fakeFile(1, 200, 2*units.MiB, 200))
	helper.Merge(fakeFile(2, 100, 3*units.MiB, 300))
	helper.Merge(fakeFile(3, 100, 10*units.MiB, 100000))
	// different regions, no split happens
	err := helper.Split(ctx)
	require.NoError(t, err)
	regions, err := mockPDCli.ScanRegions(ctx, []byte{}, []byte{}, 0)
	require.NoError(t, err)
	require.Len(t, regions, 3)
	require.Equal(t, []byte{}, regions[0].Meta.StartKey)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(100)), regions[1].Meta.StartKey)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(200)), regions[2].Meta.StartKey)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(402)), regions[2].Meta.EndKey)

	helper.Merge(fakeFile(1, 300, 3*units.MiB, 10))
	helper.Merge(fakeFile(1, 400, 4*units.MiB, 10))
	// trigger to split regions for table 1
	err = helper.Split(ctx)
	require.NoError(t, err)
	regions, err = mockPDCli.ScanRegions(ctx, []byte{}, []byte{}, 0)
	require.NoError(t, err)
	require.Len(t, regions, 4)
	require.Equal(t, fakeRowKey(100, 400), kv.Key(regions[1].Meta.EndKey))
}
