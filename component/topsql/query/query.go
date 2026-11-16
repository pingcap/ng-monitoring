package query

import "context"

type Query interface {
	Records(name string, startSecs, endSecs, windowSecs, top int, instance, instanceType string, fill *[]RecordItem) error
	Summary(ctx context.Context, startSecs, endSecs, windowSecs, top int, instance, instanceType string, fill *[]SummaryItem) error
	SummaryBy(startSecs, endSecs, windowSecs, top int, instance, instanceType, by string, fill *[]SummaryByItem) error
	Instances(startSecs, endSecs int, fill *[]InstanceItem) error
	Close()
}
