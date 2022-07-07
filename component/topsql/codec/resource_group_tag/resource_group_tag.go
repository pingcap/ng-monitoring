package resource_group_tag

import "github.com/pingcap/tipb/go-tipb"

func Decode(encoded []byte) (tipb.ResourceGroupTag, error) {
	tag := tipb.ResourceGroupTag{}
	err := tag.Unmarshal(encoded)
	return tag, err
}
