package query

type Query interface {
	Records(name string, startSecs, endSecs, windowSecs, top int, instance, instanceType string, fill *[]RecordItem) error
	Summary(startSecs, endSecs, windowSecs, top int, instance, instanceType string, fill *[]SummaryItem, orderBy string) error
	SummaryBy(startSecs, endSecs, windowSecs, top int, instance, instanceType, by string, fill *[]SummaryByItem, orderBy string) error
	Instances(startSecs, endSecs int, fill *[]InstanceItem) error
	Close()
}
