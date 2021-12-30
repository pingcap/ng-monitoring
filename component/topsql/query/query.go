package query

type Query interface {
	Records(name string, startSecs, endSecs, windowSecs, top int, instance, instanceType string, fill *[]RecordItem) error
	Summary(startSecs, endSecs, windowSecs, top int, instance, instanceType string, fill *[]SummaryItem) error
	Instances(startSecs, endSecs int, fill *[]InstanceItem) error
	Close()
}
