package plan

import "strconv"

const (
	// TypeSel is the type of Selection.
	TypeSel = "Selection"
	// TypeSet is the type of Set.
	TypeSet = "Set"
	// TypeProj is the type of Projection.
	TypeProj = "Projection"
	// TypeAgg is the type of Aggregation.
	TypeAgg = "Aggregation"
	// TypeStreamAgg is the type of StreamAgg.
	TypeStreamAgg = "StreamAgg"
	// TypeHashAgg is the type of HashAgg.
	TypeHashAgg = "HashAgg"
	// TypeShow is the type of show.
	TypeShow = "Show"
	// TypeJoin is the type of Join.
	TypeJoin = "Join"
	// TypeUnion is the type of Union.
	TypeUnion = "Union"
	// TypePartitionUnion is the type of PartitionUnion
	TypePartitionUnion = "PartitionUnion"
	// TypeTableScan is the type of TableScan.
	TypeTableScan = "TableScan"
	// TypeMemTableScan is the type of TableScan.
	TypeMemTableScan = "MemTableScan"
	// TypeUnionScan is the type of UnionScan.
	TypeUnionScan = "UnionScan"
	// TypeIdxScan is the type of IndexScan.
	TypeIdxScan = "IndexScan"
	// TypeSort is the type of Sort.
	TypeSort = "Sort"
	// TypeTopN is the type of TopN.
	TypeTopN = "TopN"
	// TypeLimit is the type of Limit.
	TypeLimit = "Limit"
	// TypeHashJoin is the type of hash join.
	TypeHashJoin = "HashJoin"
	// TypeExchangeSender is the type of mpp exchanger sender.
	TypeExchangeSender = "ExchangeSender"
	// TypeExchangeReceiver is the type of mpp exchanger receiver.
	TypeExchangeReceiver = "ExchangeReceiver"
	// TypeMergeJoin is the type of merge join.
	TypeMergeJoin = "MergeJoin"
	// TypeIndexJoin is the type of index look up join.
	TypeIndexJoin = "IndexJoin"
	// TypeIndexMergeJoin is the type of index look up merge join.
	TypeIndexMergeJoin = "IndexMergeJoin"
	// TypeIndexHashJoin is the type of index nested loop hash join.
	TypeIndexHashJoin = "IndexHashJoin"
	// TypeApply is the type of Apply.
	TypeApply = "Apply"
	// TypeMaxOneRow is the type of MaxOneRow.
	TypeMaxOneRow = "MaxOneRow"
	// TypeExists is the type of Exists.
	TypeExists = "Exists"
	// TypeDual is the type of TableDual.
	TypeDual = "TableDual"
	// TypeLock is the type of SelectLock.
	TypeLock = "SelectLock"
	// TypeInsert is the type of Insert
	TypeInsert = "Insert"
	// TypeUpdate is the type of Update.
	TypeUpdate = "Update"
	// TypeDelete is the type of Delete.
	TypeDelete = "Delete"
	// TypeIndexLookUp is the type of IndexLookUp.
	TypeIndexLookUp = "IndexLookUp"
	// TypeTableReader is the type of TableReader.
	TypeTableReader = "TableReader"
	// TypeIndexReader is the type of IndexReader.
	TypeIndexReader = "IndexReader"
	// TypeWindow is the type of Window.
	TypeWindow = "Window"
	// TypeShuffle is the type of Shuffle.
	TypeShuffle = "Shuffle"
	// TypeShuffleReceiver is the type of Shuffle.
	TypeShuffleReceiver = "ShuffleReceiver"
	// TypeTiKVSingleGather is the type of TiKVSingleGather.
	TypeTiKVSingleGather = "TiKVSingleGather"
	// TypeIndexMerge is the type of IndexMergeReader
	TypeIndexMerge = "IndexMerge"
	// TypePointGet is the type of PointGetPlan.
	TypePointGet = "Point_Get"
	// TypeShowDDLJobs is the type of show ddl jobs.
	TypeShowDDLJobs = "ShowDDLJobs"
	// TypeBatchPointGet is the type of BatchPointGetPlan.
	TypeBatchPointGet = "Batch_Point_Get"
	// TypeClusterMemTableReader is the type of TableReader.
	TypeClusterMemTableReader = "ClusterMemTableReader"
	// TypeDataSource is the type of DataSource.
	TypeDataSource = "DataSource"
	// TypeLoadData is the type of LoadData.
	TypeLoadData = "LoadData"
	// TypeTableSample is the type of TableSample.
	TypeTableSample = "TableSample"
	// TypeTableFullScan is the type of TableFullScan.
	TypeTableFullScan = "TableFullScan"
	// TypeTableRangeScan is the type of TableRangeScan.
	TypeTableRangeScan = "TableRangeScan"
	// TypeTableRowIDScan is the type of TableRowIDScan.
	TypeTableRowIDScan = "TableRowIDScan"
	// TypeIndexFullScan is the type of IndexFullScan.
	TypeIndexFullScan = "IndexFullScan"
	// TypeIndexRangeScan is the type of IndexRangeScan.
	TypeIndexRangeScan = "IndexRangeScan"
	// TypeCTETable is the type of TypeCTETable.
	TypeCTETable = "CTETable"
	// TypeCTE is the type of CTEFullScan.
	TypeCTE = "CTEFullScan"
	// TypeCTEDefinition is the type of CTE definition
	TypeCTEDefinition = "CTE"
)

// plan id.
// Attention: for compatibility of encode/decode plan, The plan id shouldn't be changed.
const (
	typeSelID                 int = 1
	typeSetID                 int = 2
	typeProjID                int = 3
	typeAggID                 int = 4
	typeStreamAggID           int = 5
	typeHashAggID             int = 6
	typeShowID                int = 7
	typeJoinID                int = 8
	typeUnionID               int = 9
	typeTableScanID           int = 10
	typeMemTableScanID        int = 11
	typeUnionScanID           int = 12
	typeIdxScanID             int = 13
	typeSortID                int = 14
	typeTopNID                int = 15
	typeLimitID               int = 16
	typeHashJoinID            int = 17
	typeMergeJoinID           int = 18
	typeIndexJoinID           int = 19
	typeIndexMergeJoinID      int = 20
	typeIndexHashJoinID       int = 21
	typeApplyID               int = 22
	typeMaxOneRowID           int = 23
	typeExistsID              int = 24
	typeDualID                int = 25
	typeLockID                int = 26
	typeInsertID              int = 27
	typeUpdateID              int = 28
	typeDeleteID              int = 29
	typeIndexLookUpID         int = 30
	typeTableReaderID         int = 31
	typeIndexReaderID         int = 32
	typeWindowID              int = 33
	typeTiKVSingleGatherID    int = 34
	typeIndexMergeID          int = 35
	typePointGet              int = 36
	typeShowDDLJobs           int = 37
	typeBatchPointGet         int = 38
	typeClusterMemTableReader int = 39
	typeDataSourceID          int = 40
	typeLoadDataID            int = 41
	typeTableSampleID         int = 42
	typeTableFullScanID       int = 43
	typeTableRangeScanID      int = 44
	typeTableRowIDScanID      int = 45
	typeIndexFullScanID       int = 46
	typeIndexRangeScanID      int = 47
	typeExchangeReceiverID    int = 48
	typeExchangeSenderID      int = 49
	typeCTEID                 int = 50
	typeCTEDefinitionID       int = 51
	typeCTETableID            int = 52
	typePartitionUnionID      int = 53
	typeShuffleID             int = 54
	typeShuffleReceiverID     int = 55
)

// PhysicalIDToTypeString converts the plan id to plan type string.
func PhysicalIDToTypeString(id int) string {
	switch id {
	case typeSelID:
		return TypeSel
	case typeSetID:
		return TypeSet
	case typeProjID:
		return TypeProj
	case typeAggID:
		return TypeAgg
	case typeStreamAggID:
		return TypeStreamAgg
	case typeHashAggID:
		return TypeHashAgg
	case typeShowID:
		return TypeShow
	case typeJoinID:
		return TypeJoin
	case typeUnionID:
		return TypeUnion
	case typePartitionUnionID:
		return TypePartitionUnion
	case typeTableScanID:
		return TypeTableScan
	case typeMemTableScanID:
		return TypeMemTableScan
	case typeUnionScanID:
		return TypeUnionScan
	case typeIdxScanID:
		return TypeIdxScan
	case typeSortID:
		return TypeSort
	case typeTopNID:
		return TypeTopN
	case typeLimitID:
		return TypeLimit
	case typeHashJoinID:
		return TypeHashJoin
	case typeMergeJoinID:
		return TypeMergeJoin
	case typeIndexJoinID:
		return TypeIndexJoin
	case typeIndexMergeJoinID:
		return TypeIndexMergeJoin
	case typeIndexHashJoinID:
		return TypeIndexHashJoin
	case typeApplyID:
		return TypeApply
	case typeMaxOneRowID:
		return TypeMaxOneRow
	case typeExistsID:
		return TypeExists
	case typeDualID:
		return TypeDual
	case typeLockID:
		return TypeLock
	case typeInsertID:
		return TypeInsert
	case typeUpdateID:
		return TypeUpdate
	case typeDeleteID:
		return TypeDelete
	case typeIndexLookUpID:
		return TypeIndexLookUp
	case typeTableReaderID:
		return TypeTableReader
	case typeIndexReaderID:
		return TypeIndexReader
	case typeWindowID:
		return TypeWindow
	case typeShuffleID:
		return TypeShuffle
	case typeShuffleReceiverID:
		return TypeShuffleReceiver
	case typeTiKVSingleGatherID:
		return TypeTiKVSingleGather
	case typeIndexMergeID:
		return TypeIndexMerge
	case typePointGet:
		return TypePointGet
	case typeShowDDLJobs:
		return TypeShowDDLJobs
	case typeBatchPointGet:
		return TypeBatchPointGet
	case typeClusterMemTableReader:
		return TypeClusterMemTableReader
	case typeDataSourceID:
		return TypeDataSource
	case typeLoadDataID:
		return TypeLoadData
	case typeTableSampleID:
		return TypeTableSample
	case typeTableFullScanID:
		return TypeTableFullScan
	case typeTableRangeScanID:
		return TypeTableRangeScan
	case typeTableRowIDScanID:
		return TypeTableRowIDScan
	case typeIndexFullScanID:
		return TypeIndexFullScan
	case typeIndexRangeScanID:
		return TypeIndexRangeScan
	case typeExchangeReceiverID:
		return TypeExchangeReceiver
	case typeExchangeSenderID:
		return TypeExchangeSender
	case typeCTEID:
		return TypeCTE
	case typeCTEDefinitionID:
		return TypeCTEDefinition
	case typeCTETableID:
		return TypeCTETable
	}

	// Should never reach here.
	return "UnknownPlanID" + strconv.Itoa(id)
}
