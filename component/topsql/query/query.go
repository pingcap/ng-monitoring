package query

type Query interface {
	TopSQL(name string, startSecs, endSecs, windowSecs, top int, instance, instanceType string, fill *[]TopSQLItem) error
	AllInstances(fill *[]InstanceItem) error
	Close()
}
