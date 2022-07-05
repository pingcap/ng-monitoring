package plan

import (
	"bytes"
	"encoding/base64"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/snappy"
	"github.com/pingcap/errors"
	"github.com/pingcap/ng-monitoring/component/topsql/codec/plan/kv"
	"github.com/pingcap/ng-monitoring/component/topsql/codec/plan/texttree"
)

func Decode(planString string) (string, error) {
	if len(planString) == 0 {
		return "", nil
	}
	pd := decoderPool.Get().(*planDecoder)
	defer decoderPool.Put(pd)
	pd.buf.Reset()
	pd.addHeader = false
	return pd.decode(planString)
}

// Following code is ported from https://github.com/pingcap/tidb/blob/7755d25aba/util/plancodec/codec.go
//
// We should try our best to catch upstream changes to avoid breaking the logic, although the upstream
// code is almost constant.

const (
	rootTaskType = "0"
)

const (
	idSeparator    = "_"
	lineBreaker    = '\n'
	lineBreakerStr = "\n"
	separator      = '\t'
	separatorStr   = "\t"
)

var (
	// PlanDiscardedEncoded indicates the discard plan because it is too long
	PlanDiscardedEncoded = "[discard]"
	planDiscardedDecoded = "(plan discarded because too long)"
)

var decoderPool = sync.Pool{
	New: func() interface{} {
		return &planDecoder{}
	},
}

type planDecoder struct {
	buf              bytes.Buffer
	depths           []int
	indents          [][]rune
	planInfos        []*planInfo
	addHeader        bool
	cacheParentIdent map[int]int
}

type planInfo struct {
	depth  int
	fields []string
}

func (pd *planDecoder) decode(planString string) (string, error) {
	str, err := decompress(planString)
	if err != nil {
		if planString == PlanDiscardedEncoded {
			return planDiscardedDecoded, nil
		}
		return "", err
	}
	return pd.buildPlanTree(str)
}

func (pd *planDecoder) buildPlanTree(planString string) (string, error) {
	nodes := strings.Split(planString, lineBreakerStr)
	if len(pd.depths) < len(nodes) {
		pd.depths = make([]int, 0, len(nodes))
		pd.planInfos = make([]*planInfo, 0, len(nodes))
		pd.indents = make([][]rune, 0, len(nodes))
	}
	pd.depths = pd.depths[:0]
	pd.planInfos = pd.planInfos[:0]
	for _, node := range nodes {
		p, err := decodePlanInfo(node)
		if err != nil {
			return "", err
		}
		if p == nil {
			continue
		}
		pd.planInfos = append(pd.planInfos, p)
		pd.depths = append(pd.depths, p.depth)
	}

	if pd.addHeader {
		pd.addPlanHeader()
	}

	// Calculated indentation of plans.
	pd.initPlanTreeIndents()
	pd.cacheParentIdent = make(map[int]int)
	for i := 1; i < len(pd.depths); i++ {
		parentIndex := pd.findParentIndex(i)
		pd.fillIndent(parentIndex, i)
	}

	// Align the value of plan fields.
	pd.alignFields()

	for i, p := range pd.planInfos {
		if i > 0 {
			pd.buf.WriteByte(lineBreaker)
		}
		// This is for alignment.
		pd.buf.WriteByte(separator)
		pd.buf.WriteString(string(pd.indents[i]))
		for j := 0; j < len(p.fields); j++ {
			if j > 0 {
				pd.buf.WriteByte(separator)
			}
			pd.buf.WriteString(p.fields[j])
		}
	}
	return pd.buf.String(), nil
}

func (pd *planDecoder) addPlanHeader() {
	if len(pd.planInfos) == 0 {
		return
	}
	header := &planInfo{
		depth:  0,
		fields: []string{"id", "task", "estRows", "operator info", "actRows", "execution info", "memory", "disk"},
	}
	if len(pd.planInfos[0].fields) < len(header.fields) {
		// plan without runtime information.
		header.fields = header.fields[:len(pd.planInfos[0].fields)]
	}
	planInfos := make([]*planInfo, 0, len(pd.planInfos)+1)
	depths := make([]int, 0, len(pd.planInfos)+1)
	planInfos = append(planInfos, header)
	planInfos = append(planInfos, pd.planInfos...)
	depths = append(depths, header.depth)
	depths = append(depths, pd.depths...)
	pd.planInfos = planInfos
	pd.depths = depths
}

func (pd *planDecoder) initPlanTreeIndents() {
	pd.indents = pd.indents[:0]
	for i := 0; i < len(pd.depths); i++ {
		indent := make([]rune, 2*pd.depths[i])
		pd.indents = append(pd.indents, indent)
		if len(indent) == 0 {
			continue
		}
		for i := 0; i < len(indent)-2; i++ {
			indent[i] = ' '
		}
		indent[len(indent)-2] = texttree.TreeLastNode
		indent[len(indent)-1] = texttree.TreeNodeIdentifier
	}
}

func (pd *planDecoder) findParentIndex(childIndex int) int {
	pd.cacheParentIdent[pd.depths[childIndex]] = childIndex
	parentDepth := pd.depths[childIndex] - 1
	if parentIdx, ok := pd.cacheParentIdent[parentDepth]; ok {
		return parentIdx
	}
	for i := childIndex - 1; i > 0; i-- {
		if pd.depths[i] == parentDepth {
			pd.cacheParentIdent[pd.depths[i]] = i
			return i
		}
	}
	return 0
}

func (pd *planDecoder) fillIndent(parentIndex, childIndex int) {
	depth := pd.depths[childIndex]
	if depth == 0 {
		return
	}
	idx := depth*2 - 2
	for i := childIndex - 1; i > parentIndex; i-- {
		if pd.indents[i][idx] == texttree.TreeLastNode {
			pd.indents[i][idx] = texttree.TreeMiddleNode
			break
		}
		pd.indents[i][idx] = texttree.TreeBody
	}
}

func (pd *planDecoder) alignFields() {
	if len(pd.planInfos) == 0 {
		return
	}
	// Align fields length. Some plan may doesn't have runtime info, need append `` to align with other plan fields.
	maxLen := -1
	for _, p := range pd.planInfos {
		if len(p.fields) > maxLen {
			maxLen = len(p.fields)
		}
	}
	for _, p := range pd.planInfos {
		for len(p.fields) < maxLen {
			p.fields = append(p.fields, "")
		}
	}

	fieldsLen := len(pd.planInfos[0].fields)
	// Last field no need to align.
	fieldsLen--
	var buf []byte
	for colIdx := 0; colIdx < fieldsLen; colIdx++ {
		maxFieldLen := pd.getMaxFieldLength(colIdx)
		for rowIdx, p := range pd.planInfos {
			fillLen := maxFieldLen - pd.getPlanFieldLen(rowIdx, colIdx, p)
			for len(buf) < fillLen {
				buf = append(buf, ' ')
			}
			buf = buf[:fillLen]
			p.fields[colIdx] += string(buf)
		}
	}
}

func (pd *planDecoder) getMaxFieldLength(idx int) int {
	maxLength := -1
	for rowIdx, p := range pd.planInfos {
		l := pd.getPlanFieldLen(rowIdx, idx, p)
		if l > maxLength {
			maxLength = l
		}
	}
	return maxLength
}

func (pd *planDecoder) getPlanFieldLen(rowIdx, colIdx int, p *planInfo) int {
	if colIdx == 0 {
		return len(p.fields[0]) + len(pd.indents[rowIdx])
	}
	return len(p.fields[colIdx])
}

func decodePlanInfo(str string) (*planInfo, error) {
	values := strings.Split(str, separatorStr)
	if len(values) < 2 {
		return nil, nil
	}

	p := &planInfo{
		fields: make([]string, 0, len(values)-1),
	}
	for i, v := range values {
		switch i {
		// depth
		case 0:
			depth, err := strconv.Atoi(v)
			if err != nil {
				return nil, errors.Errorf("decode plan: %v, depth: %v, error: %v", str, v, err)
			}
			p.depth = depth
		// plan ID
		case 1:
			ids := strings.Split(v, idSeparator)
			if len(ids) != 1 && len(ids) != 2 {
				return nil, errors.Errorf("decode plan: %v error, invalid plan id: %v", str, v)
			}
			planID, err := strconv.Atoi(ids[0])
			if err != nil {
				return nil, errors.Errorf("decode plan: %v, plan id: %v, error: %v", str, v, err)
			}
			if len(ids) == 1 {
				p.fields = append(p.fields, PhysicalIDToTypeString(planID))
			} else {
				p.fields = append(p.fields, PhysicalIDToTypeString(planID)+idSeparator+ids[1])
			}
		// task type
		case 2:
			task, err := decodeTaskType(v)
			if err != nil {
				return nil, errors.Errorf("decode plan: %v, task type: %v, error: %v", str, v, err)
			}
			p.fields = append(p.fields, task)
		default:
			p.fields = append(p.fields, v)
		}
	}
	return p, nil
}

func decodeTaskType(str string) (string, error) {
	segs := strings.Split(str, idSeparator)
	if segs[0] == rootTaskType {
		return "root", nil
	}
	if len(segs) == 1 { // be compatible to `NormalizePlanNode`, which doesn't encode storeType in task field.
		return "cop", nil
	}
	storeType, err := strconv.Atoi(segs[1])
	if err != nil {
		return "", err
	}
	return "cop[" + ((kv.StoreType)(storeType)).Name() + "]", nil
}

func decompress(str string) (string, error) {
	decodeBytes, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return "", err
	}

	bs, err := snappy.Decode(nil, decodeBytes)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}
