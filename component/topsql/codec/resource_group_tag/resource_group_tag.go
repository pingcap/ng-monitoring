package resource_group_tag

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

func Decode(encoded []byte) (tipb.ResourceGroupTag, error) {
	tag := tipb.ResourceGroupTag{}
	err := tag.Unmarshal(encoded)
	log.Warn("xxx-------------------------------------------- tag", zap.ByteString("keyspace name", tag.KeyspaceName), zap.Int64("tid", tag.TableId))
	return tag, err
}
