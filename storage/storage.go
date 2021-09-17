package storage

import (
    "github.com/zhongzc/diag_backend/storage/database"
    "github.com/zhongzc/diag_backend/storage/database/document"
    "github.com/zhongzc/diag_backend/storage/query"
    "github.com/zhongzc/diag_backend/storage/store"
)

func Init(httpAddr string) {
    database.Init()

    store.Init(httpAddr, document.Get())
    query.Init(httpAddr, document.Get())
}

func Stop() {
    database.Stop()
}
