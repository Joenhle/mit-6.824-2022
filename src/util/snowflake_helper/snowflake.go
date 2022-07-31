package snowflake_helper

import (
	"github.com/bwmarrin/snowflake"
	"log"
)

var node *snowflake.Node

func GenID() int64 {
	return node.Generate().Int64()
}

func init() {
	var err error
	node, err = snowflake.NewNode(1)
	if err != nil {
		log.Fatalf("生成全局id失败")
		return
	}
}
