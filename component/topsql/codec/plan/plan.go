package plan

import (
	"encoding/base64"

	"github.com/golang/snappy"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

func Decode(planString string) (string, error) {
	binaryPlan, err := decompress(planString)
	if err != nil {
		return "", err
	}

	return plancodec.DecodeNormalizedPlan(binaryPlan)
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
