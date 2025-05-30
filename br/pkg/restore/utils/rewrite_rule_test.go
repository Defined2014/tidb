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

package utils_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func TestValidateFileRewriteRule(t *testing.T) {
	rules := &utils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{{
			OldKeyPrefix: []byte(tablecodec.EncodeTablePrefix(1)),
			NewKeyPrefix: []byte(tablecodec.EncodeTablePrefix(2)),
		}},
	}

	// Empty start/end key is not allowed.
	err := utils.ValidateFileRewriteRule(
		&backuppb.File{
			Name:     "file_write.sst",
			StartKey: []byte(""),
			EndKey:   []byte(""),
		},
		rules,
	)
	require.Error(t, err)
	require.Regexp(t, ".*cannot find rewrite rule.*", err.Error())

	// Range is not overlap, no rule found.
	err = utils.ValidateFileRewriteRule(
		&backuppb.File{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(0),
			EndKey:   tablecodec.EncodeTablePrefix(1),
		},
		rules,
	)
	require.Error(t, err)
	require.Regexp(t, ".*cannot find rewrite rule.*", err.Error())

	// No rule for end key.
	err = utils.ValidateFileRewriteRule(
		&backuppb.File{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(2),
		},
		rules,
	)
	require.Error(t, err)
	require.Regexp(t, ".*cannot find rewrite rule.*", err.Error())

	// Add a rule for end key.
	rules.Data = append(rules.Data, &import_sstpb.RewriteRule{
		OldKeyPrefix: tablecodec.EncodeTablePrefix(2),
		NewKeyPrefix: tablecodec.EncodeTablePrefix(3),
	})
	err = utils.ValidateFileRewriteRule(
		&backuppb.File{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(2),
		},
		rules,
	)
	require.Error(t, err)
	require.Regexp(t, ".*rewrite rule mismatch.*", err.Error())

	// Add a bad rule for end key, after rewrite start key > end key.
	rules.Data = append(rules.Data[:1], &import_sstpb.RewriteRule{
		OldKeyPrefix: tablecodec.EncodeTablePrefix(2),
		NewKeyPrefix: tablecodec.EncodeTablePrefix(1),
	})
	err = utils.ValidateFileRewriteRule(
		&backuppb.File{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(2),
		},
		rules,
	)
	require.Error(t, err)
	require.Regexp(t, ".*rewrite rule mismatch.*", err.Error())
}

func TestRewriteFileKeys(t *testing.T) {
	rewriteRules := utils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				NewKeyPrefix: tablecodec.GenTablePrefix(2),
				OldKeyPrefix: tablecodec.GenTablePrefix(1),
			},
			{
				NewKeyPrefix: tablecodec.GenTablePrefix(511),
				OldKeyPrefix: tablecodec.GenTablePrefix(767),
			},
		},
	}
	rawKeyFile := backuppb.File{
		Name:     "backup.sst",
		StartKey: tablecodec.GenTableRecordPrefix(1),
		EndKey:   tablecodec.GenTableRecordPrefix(1).PrefixNext(),
	}
	start, end, err := utils.GetRewriteRawKeys(&rawKeyFile, &rewriteRules)
	require.NoError(t, err)
	_, end, err = codec.DecodeBytes(end, nil)
	require.NoError(t, err)
	_, start, err = codec.DecodeBytes(start, nil)
	require.NoError(t, err)
	require.Equal(t, []byte(tablecodec.GenTableRecordPrefix(2)), start)
	require.Equal(t, []byte(tablecodec.GenTableRecordPrefix(2).PrefixNext()), end)

	encodeKeyFile := backuppb.DataFileInfo{
		Path:     "bakcup.log",
		StartKey: codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(1)),
		EndKey:   codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(1).PrefixNext()),
	}
	start, end, err = utils.GetRewriteEncodedKeys(&encodeKeyFile, &rewriteRules)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(2)), start)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(2).PrefixNext()), end)

	// test for table id 767
	encodeKeyFile767 := backuppb.DataFileInfo{
		Path:     "bakcup.log",
		StartKey: codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(767)),
		EndKey:   codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(767).PrefixNext()),
	}
	// use raw rewrite should no error but not equal
	start, end, err = utils.GetRewriteRawKeys(&encodeKeyFile767, &rewriteRules)
	require.NoError(t, err)
	require.NotEqual(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(511)), start)
	require.NotEqual(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(511).PrefixNext()), end)
	// use encode rewrite should no error and equal
	start, end, err = utils.GetRewriteEncodedKeys(&encodeKeyFile767, &rewriteRules)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(511)), start)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(511).PrefixNext()), end)
}

func TestRewriteRange(t *testing.T) {
	// Define test cases
	cases := []struct {
		rg            *rtree.Range
		rewriteRules  *utils.RewriteRules
		expectedRange *rtree.Range
		expectedError error
	}{
		// Test case 1: No rewrite rules
		{
			rg: &rtree.Range{
				KeyRange: rtree.KeyRange{
					StartKey: []byte("startKey"),
					EndKey:   []byte("endKey"),
				},
			},
			rewriteRules:  nil,
			expectedRange: &rtree.Range{KeyRange: rtree.KeyRange{StartKey: []byte("startKey"), EndKey: []byte("endKey")}},
			expectedError: nil,
		},
		// Test case 2: Rewrite rule found for both start key and end key
		{
			rg: &rtree.Range{
				KeyRange: rtree.KeyRange{
					StartKey: append(tablecodec.GenTableIndexPrefix(1), []byte("startKey")...),
					EndKey:   append(tablecodec.GenTableIndexPrefix(1), []byte("endKey")...),
				},
			},
			rewriteRules: &utils.RewriteRules{
				Data: []*import_sstpb.RewriteRule{
					{
						OldKeyPrefix: tablecodec.GenTableIndexPrefix(1),
						NewKeyPrefix: tablecodec.GenTableIndexPrefix(2),
					},
				},
			},
			expectedRange: &rtree.Range{
				KeyRange: rtree.KeyRange{
					StartKey: append(tablecodec.GenTableIndexPrefix(2), []byte("startKey")...),
					EndKey:   append(tablecodec.GenTableIndexPrefix(2), []byte("endKey")...),
				},
			},
			expectedError: nil,
		},
		// Test case 3: Rewrite rule found for end key
		{
			rg: &rtree.Range{
				KeyRange: rtree.KeyRange{
					StartKey: append(tablecodec.GenTableIndexPrefix(1), []byte("startKey")...),
					EndKey:   append(tablecodec.GenTableIndexPrefix(1), []byte("endKey")...),
				},
			},
			rewriteRules: &utils.RewriteRules{
				Data: []*import_sstpb.RewriteRule{
					{
						OldKeyPrefix: append(tablecodec.GenTableIndexPrefix(1), []byte("endKey")...),
						NewKeyPrefix: append(tablecodec.GenTableIndexPrefix(2), []byte("newEndKey")...),
					},
				},
			},
			expectedRange: &rtree.Range{
				KeyRange: rtree.KeyRange{
					StartKey: append(tablecodec.GenTableIndexPrefix(1), []byte("startKey")...),
					EndKey:   append(tablecodec.GenTableIndexPrefix(2), []byte("newEndKey")...),
				},
			},
			expectedError: nil,
		},
		// Test case 4: Table ID mismatch
		{
			rg: &rtree.Range{
				KeyRange: rtree.KeyRange{
					StartKey: []byte("t1_startKey"),
					EndKey:   []byte("t2_endKey"),
				},
			},
			rewriteRules: &utils.RewriteRules{
				Data: []*import_sstpb.RewriteRule{
					{
						OldKeyPrefix: []byte("t1_startKey"),
						NewKeyPrefix: []byte("t2_newStartKey"),
					},
				},
			},
			expectedRange: nil,
			expectedError: errors.Annotate(berrors.ErrRestoreTableIDMismatch, "table id mismatch"),
		},
	}

	// Run test cases
	for _, tc := range cases {
		actualRange, actualError := utils.RewriteRange(tc.rg, tc.rewriteRules)
		if tc.expectedError != nil {
			require.EqualError(t, tc.expectedError, actualError.Error())
		} else {
			require.NoError(t, actualError)
		}
		require.Equal(t, tc.expectedRange, actualRange)
	}
}

func TestGetRewriteTableID(t *testing.T) {
	var tableID int64 = 76
	var oldTableID int64 = 80
	{
		rewriteRules := &utils.RewriteRules{
			Data: []*import_sstpb.RewriteRule{
				{
					OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTableID),
					NewKeyPrefix: tablecodec.EncodeTablePrefix(tableID),
				},
			},
		}

		newTableID := utils.GetRewriteTableID(oldTableID, rewriteRules)
		require.Equal(t, tableID, newTableID)
	}

	{
		rewriteRules := &utils.RewriteRules{
			Data: []*import_sstpb.RewriteRule{
				{
					OldKeyPrefix: tablecodec.GenTableRecordPrefix(oldTableID),
					NewKeyPrefix: tablecodec.GenTableRecordPrefix(tableID),
				},
			},
		}

		newTableID := utils.GetRewriteTableID(oldTableID, rewriteRules)
		require.Equal(t, tableID, newTableID)
	}
}

func getNewKeyPrefix(key []byte, rewriteRules *utils.RewriteRules) kv.Key {
	for _, rule := range rewriteRules.Data {
		if bytes.HasPrefix(key, rule.GetOldKeyPrefix()) {
			return rule.GetNewKeyPrefix()
		}
	}
	return nil
}

func generateRewriteTableInfos() (newTableInfo, oldTableInfo *model.TableInfo) {
	newTableInfo = &model.TableInfo{
		ID: 1,
		Indices: []*model.IndexInfo{
			{
				ID:   1,
				Name: ast.NewCIStr("i1"),
			},
			{
				ID:   2,
				Name: ast.NewCIStr("i2"),
			},
		},
		Partition: &model.PartitionInfo{
			Definitions: []model.PartitionDefinition{
				{
					ID:   100,
					Name: ast.NewCIStr("p1"),
				},
				{
					ID:   200,
					Name: ast.NewCIStr("p2"),
				},
			},
		},
	}
	oldTableInfo = &model.TableInfo{
		ID: 2,
		Indices: []*model.IndexInfo{
			{
				ID:   1,
				Name: ast.NewCIStr("i1"),
			},
			{
				ID:   2,
				Name: ast.NewCIStr("i2"),
			},
		},
		Partition: &model.PartitionInfo{
			Definitions: []model.PartitionDefinition{
				{
					ID:   101,
					Name: ast.NewCIStr("p1"),
				},
				{
					ID:   201,
					Name: ast.NewCIStr("p2"),
				},
			},
		},
	}
	return newTableInfo, oldTableInfo
}

func TestGetRewriteRules(t *testing.T) {
	newTableInfo, oldTableInfo := generateRewriteTableInfos()

	{
		rewriteRules := utils.GetRewriteRules(newTableInfo, oldTableInfo, 0, false)
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTablePrefix(2), rewriteRules), tablecodec.EncodeTablePrefix(1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTablePrefix(101), rewriteRules), tablecodec.EncodeTablePrefix(100))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTablePrefix(201), rewriteRules), tablecodec.EncodeTablePrefix(200))
	}

	{
		rewriteRules := utils.GetRewriteRules(newTableInfo, oldTableInfo, 0, true)
		require.Equal(t, getNewKeyPrefix(tablecodec.GenTableRecordPrefix(2), rewriteRules), tablecodec.GenTableRecordPrefix(1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(2, 1), rewriteRules), tablecodec.EncodeTableIndexPrefix(1, 1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(2, 2), rewriteRules), tablecodec.EncodeTableIndexPrefix(1, 2))
		require.Equal(t, getNewKeyPrefix(tablecodec.GenTableRecordPrefix(101), rewriteRules), tablecodec.GenTableRecordPrefix(100))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(101, 1), rewriteRules), tablecodec.EncodeTableIndexPrefix(100, 1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(101, 2), rewriteRules), tablecodec.EncodeTableIndexPrefix(100, 2))
		require.Equal(t, getNewKeyPrefix(tablecodec.GenTableRecordPrefix(201), rewriteRules), tablecodec.GenTableRecordPrefix(200))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(201, 1), rewriteRules), tablecodec.EncodeTableIndexPrefix(200, 1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(201, 2), rewriteRules), tablecodec.EncodeTableIndexPrefix(200, 2))
	}
}

func TestGetRewriteRulesMap(t *testing.T) {
	newTableInfo, oldTableInfo := generateRewriteTableInfos()

	{
		rewriteRules := utils.GetRewriteRulesMap(newTableInfo, oldTableInfo, 0, false)
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTablePrefix(2), rewriteRules[2]), tablecodec.EncodeTablePrefix(1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTablePrefix(101), rewriteRules[101]), tablecodec.EncodeTablePrefix(100))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTablePrefix(201), rewriteRules[201]), tablecodec.EncodeTablePrefix(200))
	}

	{
		rewriteRules := utils.GetRewriteRulesMap(newTableInfo, oldTableInfo, 0, true)
		require.Equal(t, getNewKeyPrefix(tablecodec.GenTableRecordPrefix(2), rewriteRules[2]), tablecodec.GenTableRecordPrefix(1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(2, 1), rewriteRules[2]), tablecodec.EncodeTableIndexPrefix(1, 1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(2, 2), rewriteRules[2]), tablecodec.EncodeTableIndexPrefix(1, 2))
		require.Equal(t, getNewKeyPrefix(tablecodec.GenTableRecordPrefix(101), rewriteRules[101]), tablecodec.GenTableRecordPrefix(100))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(101, 1), rewriteRules[101]), tablecodec.EncodeTableIndexPrefix(100, 1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(101, 2), rewriteRules[101]), tablecodec.EncodeTableIndexPrefix(100, 2))
		require.Equal(t, getNewKeyPrefix(tablecodec.GenTableRecordPrefix(201), rewriteRules[201]), tablecodec.GenTableRecordPrefix(200))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(201, 1), rewriteRules[201]), tablecodec.EncodeTableIndexPrefix(200, 1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(201, 2), rewriteRules[201]), tablecodec.EncodeTableIndexPrefix(200, 2))
	}
}

func TestGetRewriteRuleOfTable(t *testing.T) {
	// Test basic table prefix rewrite without detailed rules
	{
		rewriteRules := utils.GetRewriteRuleOfTable(2, 1, map[int64]int64{1: 1, 2: 2}, false)
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTablePrefix(2), rewriteRules), tablecodec.EncodeTablePrefix(1))
		require.Len(t, rewriteRules.Data, 1) // Only one rule for table prefix
		require.Equal(t, rewriteRules.NewTableID, int64(1))
		require.Equal(t, rewriteRules.TableIDRemapHint, []utils.TableIDRemap{{Origin: 2, Rewritten: 1}})
	}

	// Test detailed rules including record and index prefixes
	{
		indexIDs := map[int64]int64{1: 1, 2: 2}
		rewriteRules := utils.GetRewriteRuleOfTable(2, 1, indexIDs, true)
		// Check record prefix
		require.Equal(t, getNewKeyPrefix(tablecodec.GenTableRecordPrefix(2), rewriteRules), tablecodec.GenTableRecordPrefix(1))
		// Check index prefixes
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(2, 1), rewriteRules), tablecodec.EncodeTableIndexPrefix(1, 1))
		require.Equal(t, getNewKeyPrefix(tablecodec.EncodeTableIndexPrefix(2, 2), rewriteRules), tablecodec.EncodeTableIndexPrefix(1, 2))
		// Verify number of rules (1 record + 2 index rules)
		require.Len(t, rewriteRules.Data, 3)
	}

	// Test timestamp fields
	{
		shiftStartTs := uint64(30)
		startTs := uint64(50)
		restoredTs := uint64(100)
		rewriteRules := utils.GetRewriteRuleOfTable(2, 1, map[int64]int64{1: 1}, true)
		rewriteRules.SetTsRange(shiftStartTs, startTs, restoredTs)

		// Verify timestamp fields in RewriteRules
		require.Equal(t, restoredTs, rewriteRules.RestoredTs)
		require.Equal(t, startTs, rewriteRules.StartTs)
		require.Equal(t, shiftStartTs, rewriteRules.ShiftStartTs)

		// Verify TableIDRemapHint
		require.Equal(t, []utils.TableIDRemap{{Origin: 2, Rewritten: 1}}, rewriteRules.TableIDRemapHint)

		// Verify NewTableID
		require.Equal(t, int64(1), rewriteRules.NewTableID)
	}

	// Test with empty index IDs
	{
		rewriteRules := utils.GetRewriteRuleOfTable(2, 1, map[int64]int64{}, true)
		require.Len(t, rewriteRules.Data, 1) // Only record rule, no index rules
		require.Equal(t, getNewKeyPrefix(tablecodec.GenTableRecordPrefix(2), rewriteRules), tablecodec.GenTableRecordPrefix(1))
	}
}

type fakeApplyFile struct {
	StartKey []byte
	EndKey   []byte
}

func (f fakeApplyFile) GetStartKey() []byte {
	return f.StartKey
}

func (f fakeApplyFile) GetEndKey() []byte {
	return f.EndKey
}

func rewriteKey(key kv.Key, rule *import_sstpb.RewriteRule) kv.Key {
	if bytes.HasPrefix(key, rule.GetOldKeyPrefix()) {
		return append(rule.GetNewKeyPrefix(), key[len(rule.GetNewKeyPrefix()):]...)
	}
	return nil
}

func TestFindMatchedRewriteRule(t *testing.T) {
	rewriteRules := utils.GetRewriteRuleOfTable(2, 1, map[int64]int64{1: 10}, true)
	{
		applyFile := fakeApplyFile{
			StartKey: tablecodec.EncodeRowKeyWithHandle(2, kv.IntHandle(100)),
			EndKey:   tablecodec.EncodeRowKeyWithHandle(2, kv.IntHandle(200)),
		}
		rule := utils.FindMatchedRewriteRule(applyFile, rewriteRules)
		require.Equal(t, rewriteKey(tablecodec.EncodeRowKeyWithHandle(2, kv.IntHandle(100)), rule),
			tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(100)))
	}

	{
		applyFile := fakeApplyFile{
			StartKey: tablecodec.EncodeIndexSeekKey(2, 1, []byte("test-1")),
			EndKey:   tablecodec.EncodeIndexSeekKey(2, 1, []byte("test-2")),
		}
		rule := utils.FindMatchedRewriteRule(applyFile, rewriteRules)
		require.Equal(t, rewriteKey(tablecodec.EncodeIndexSeekKey(2, 1, []byte("test-1")), rule),
			tablecodec.EncodeIndexSeekKey(1, 10, []byte("test-1")))
	}

	{
		applyFile := fakeApplyFile{
			StartKey: tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(100)),
			EndKey:   tablecodec.EncodeRowKeyWithHandle(2, kv.IntHandle(200)),
		}
		rule := utils.FindMatchedRewriteRule(applyFile, rewriteRules)
		require.Nil(t, rule)
	}

	{
		applyFile := fakeApplyFile{
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(1),
		}
		rule := utils.FindMatchedRewriteRule(applyFile, rewriteRules)
		require.Nil(t, rule)
	}
}

func TestGetRewriteKeyWithDifferentTable(t *testing.T) {
	applyFile := fakeApplyFile{
		StartKey: tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(100)),
		EndKey:   tablecodec.EncodeRowKeyWithHandle(2, kv.IntHandle(200)),
	}
	_, _, err := utils.GetRewriteRawKeys(applyFile, nil)
	require.Error(t, err)
	_, _, err = utils.GetRewriteEncodedKeys(applyFile, nil)
	require.Error(t, err)
}

func TestSetTimeRangeFilter(t *testing.T) {
	testCases := []struct {
		name        string
		rules       *utils.RewriteRules
		cfName      string
		expectError bool
	}{
		{
			name: "default cf with valid timestamps",
			rules: &utils.RewriteRules{
				Data: []*import_sstpb.RewriteRule{
					{OldKeyPrefix: []byte("old"), NewKeyPrefix: []byte("new")},
				},
				ShiftStartTs: 50, // Less than StartTs
				StartTs:      100,
				RestoredTs:   200,
			},
			cfName:      "default",
			expectError: false,
		},
		{
			name: "write cf with valid timestamps",
			rules: &utils.RewriteRules{
				Data: []*import_sstpb.RewriteRule{
					{OldKeyPrefix: []byte("old"), NewKeyPrefix: []byte("new")},
				},
				ShiftStartTs: 50, // Less than StartTs
				StartTs:      100,
				RestoredTs:   200,
			},
			cfName:      "write",
			expectError: false,
		},
		{
			name: "invalid shift start ts (greater than start ts)",
			rules: &utils.RewriteRules{
				Data: []*import_sstpb.RewriteRule{
					{OldKeyPrefix: []byte("old"), NewKeyPrefix: []byte("new")},
				},
				ShiftStartTs: 150, // Greater than StartTs
				StartTs:      100,
				RestoredTs:   200,
			},
			cfName:      "default",
			expectError: false,
		},
		{
			name: "write cf valid shift start ts (greater than start ts)",
			rules: &utils.RewriteRules{
				Data: []*import_sstpb.RewriteRule{
					{OldKeyPrefix: []byte("old"), NewKeyPrefix: []byte("new")},
				},
				ShiftStartTs: 150, // Greater than StartTs
				StartTs:      100,
				RestoredTs:   200,
			},
			cfName:      "write",
			expectError: false,
		},
		{
			name: "invalid cf name",
			rules: &utils.RewriteRules{
				Data: []*import_sstpb.RewriteRule{
					{OldKeyPrefix: []byte("old"), NewKeyPrefix: []byte("new")},
				},
				ShiftStartTs: 50,
				StartTs:      100,
				RestoredTs:   200,
			},
			cfName:      "invalid",
			expectError: true,
		},
		{
			name: "zero timestamps should skip filter",
			rules: &utils.RewriteRules{
				Data: []*import_sstpb.RewriteRule{
					{OldKeyPrefix: []byte("old"), NewKeyPrefix: []byte("new")},
				},
				StartTs:      0,
				RestoredTs:   0,
				ShiftStartTs: 0,
			},
			cfName:      "default",
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rule := &import_sstpb.RewriteRule{}
			err := utils.SetTimeRangeFilter(tc.rules, rule, tc.cfName)
			if tc.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			if tc.rules.StartTs == 0 || tc.rules.RestoredTs == 0 {
				// Should not modify rules when timestamps are zero
				for _, rule := range tc.rules.Data {
					require.Zero(t, rule.IgnoreBeforeTimestamp)
					require.Zero(t, rule.IgnoreAfterTimestamp)
				}
				return
			}

			// Verify timestamps are set correctly
			require.Equal(t, tc.rules.RestoredTs, rule.IgnoreAfterTimestamp)

			if strings.Contains(tc.cfName, "default") {
				if tc.rules.ShiftStartTs < tc.rules.StartTs {
					require.Equal(t, tc.rules.ShiftStartTs, rule.IgnoreBeforeTimestamp)
				} else {
					require.Equal(t, tc.rules.StartTs, rule.IgnoreBeforeTimestamp)
				}
			} else if strings.Contains(tc.cfName, "write") {
				require.Equal(t, tc.rules.StartTs, rule.IgnoreBeforeTimestamp)
			}
		})
	}
}

func TestSetTimeRangeFilterRace(t *testing.T) {
	// Create a shared rules object that will be read by multiple goroutines
	rules := &utils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{OldKeyPrefix: []byte("old"), NewKeyPrefix: []byte("new")},
		},
		ShiftStartTs: 50,
		StartTs:      100,
		RestoredTs:   200,
	}

	// Number of concurrent goroutines
	numGoroutines := 100
	// Channel to collect results
	resultChan := make(chan *import_sstpb.RewriteRule, numGoroutines)

	// Launch multiple goroutines to concurrently call SetTimeRangeFilter
	for range numGoroutines {
		go func() {
			// Each goroutine creates its own rule
			rule := &import_sstpb.RewriteRule{}
			err := utils.SetTimeRangeFilter(rules, rule, "default")
			if err != nil {
				resultChan <- nil
				return
			}
			resultChan <- rule
		}()
	}

	// Wait for all goroutines to complete and check results
	for range numGoroutines {
		rule := <-resultChan
		require.NotNil(t, rule)
		// Verify the rule was correctly modified
		require.Equal(t, uint64(50), rule.IgnoreBeforeTimestamp) // Should be ShiftStartTs for default cf
		require.Equal(t, uint64(200), rule.IgnoreAfterTimestamp) // Should be RestoredTs
	}

	// Verify the rules were not modified
	require.Equal(t, uint64(50), rules.ShiftStartTs)
	require.Equal(t, uint64(100), rules.StartTs)
	require.Equal(t, uint64(200), rules.RestoredTs)
}
