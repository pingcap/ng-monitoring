package query

type Query interface {
	TopSQL(name string, startSecs, endSecs, windowSecs, top int, instance string, fill *[]TopSQLItem) error
	Instances(startSecs, endSecs int, fill *[]InstanceItem) error
	Close()
}
