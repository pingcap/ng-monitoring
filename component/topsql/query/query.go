package query

import "context"

type Query interface {
	Records(name string, startSecs, endSecs, windowSecs, top int, instance, instanceType string, fill *[]RecordItem) error
<<<<<<< HEAD
	Summary(startSecs, endSecs, windowSecs, top int, instance, instanceType string, fill *[]SummaryItem) error
	SummaryBy(startSecs, endSecs, windowSecs, top int, instance, instanceType, by string, fill *[]SummaryByItem) error
=======
	Summary(ctx context.Context, startSecs, endSecs, windowSecs, top int, instance, instanceType string, fill *[]SummaryItem, orderBy string) error
	SummaryBy(startSecs, endSecs, windowSecs, top int, instance, instanceType, by string, fill *[]SummaryByItem, orderBy string) error
>>>>>>> 9d2cf4d (topsql: push down By Query ranking and bound request work (#373))
	Instances(startSecs, endSecs int, fill *[]InstanceItem) error
	Close()
}
