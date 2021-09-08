package kv

import (
    "flag"

    "github.com/dgraph-io/badger/v3"
    "github.com/pingcap/log"
    "go.uber.org/zap"
)

var (
    // kvPath is a path to storage kv data, i.e. sql digest and plan digest.
    kvPath = flag.String("kvPath", "kv-data", "Path to storage kv data")
)

var kvStorage *badger.DB

func Init() {
    var err error
    kvStorage, err = badger.Open(badger.DefaultOptions(*kvPath))
    if err != nil {
        log.Fatal("cannot open a badger storage", zap.String("path", *kvPath), zap.Error(err))
    }
}

func Stop() {
    if err := kvStorage.Close(); err != nil {
        log.Fatal("cannot close the badger storage", zap.Error(err))
    }
}

type Importer struct {
    txn *badger.Txn
}

func NewImporter() Importer {
    return Importer{txn: kvStorage.NewTransaction(true)}
}

func (ipt *Importer) Commit() error {
    return ipt.txn.Commit()
}

func (ipt *Importer) Close() {
    ipt.txn.Discard()
}

func (ipt *Importer) NewTableImporter(prefix []byte) TableImporter {
    return TableImporter{
        prefix:   prefix,
        importer: ipt,
    }
}

type TableImporter struct {
    prefix   []byte
    importer *Importer
}

func (ti *TableImporter) Add(key, value string) error {
    return ti.importer.txn.Set(append(ti.prefix, key...), []byte(value))
}

func (ti *TableImporter) Delete(key string) error {
    return ti.importer.txn.Delete(append(ti.prefix, key...))
}

type Reader struct {
    txn *badger.Txn
}

func NewReader() Reader {
    return Reader{txn: kvStorage.NewTransaction(false)}
}

func (rdr *Reader) Close() {
    rdr.txn.Discard()
}

func (rdr *Reader) NewTableReader(prefix []byte) TableReader {
    return TableReader{
        prefix: prefix,
        reader: rdr,
    }
}

type TableReader struct {
    prefix []byte
    reader *Reader
}

func (tr *TableReader) Get(key string, fn func(*string)) error {
    item, err := tr.reader.txn.Get(append(tr.prefix, key...))
    if err == badger.ErrKeyNotFound {
        fn(nil)
        return nil
    }
    if err != nil {
        return err
    }

    return item.Value(func(val []byte) error {
        s := string(val)
        fn(&s)
        return nil
    })
}
