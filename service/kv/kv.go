package kv

import (
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "net/http"
    "strings"

    "github.com/pingcap/log"
    storage "github.com/zhongzc/diag_backend/kv"
    "go.uber.org/zap"
)

type kv struct {
    Key   string
    Value *string
}

type queryRequest struct {
    Queries []struct {
        Table string   `json:"table"`
        Keys  []string `json:"keys"`
    } `json:"queries"`
}

type queryResponse struct {
    Data []tableResp `json:"data"`
}

type tableResp struct {
    Table string `json:"table"`
    KVs   []kv   `json:"kvs"`
}

type importRequest struct {
    Data []struct {
        Table string `json:"table"`
        KVs   []kv   `json:"kvs"`
    } `json:"data"`
}

type importResponse struct{}

// RequestHandler is a kv storage request handler.
func RequestHandler(w http.ResponseWriter, r *http.Request) bool {
    path := strings.Replace(r.URL.Path, "//", "/", -1)

    switch path {
    case "/kv/import":
        w.Header().Set("Access-Control-Allow-Origin", "*")

        var iptReq importRequest
        d := json.NewDecoder(r.Body)
        if err := d.Decode(&iptReq); err != nil && err != io.EOF {
           http.Error(w, err.Error(), http.StatusBadRequest)
           return true
        }

        ipt := storage.NewImporter()
        defer ipt.Close()

        for _, data := range iptReq.Data {
            prefix, err := TablePrefix(data.Table)
            if err != nil {
                http.Error(w, err.Error(), http.StatusBadRequest)
                return true
            }

            ti := ipt.NewTableImporter(prefix)
            for _, kv := range data.KVs {
                if kv.Value == nil {
                    if err = ti.Delete(kv.Key); err != nil {
                        http.Error(w, err.Error(), http.StatusInternalServerError)
                        return true
                    }
                    continue
                }

                if err = ti.Add(kv.Key, *kv.Value); err != nil {
                    http.Error(w, err.Error(), http.StatusInternalServerError)
                    return true
                }
            }
        }

        if err := ipt.Commit(); err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return true
        }

        w.WriteHeader(http.StatusCreated)
        w.Header().Set("Content-Type", "application/json; charset=utf-8")
        if err := json.NewEncoder(w).Encode(importResponse{}); err != nil {
            log.Warn("failed to encode response", zap.Error(err))
        }
        return true
    case "/kv/query":
        w.Header().Set("Access-Control-Allow-Origin", "*")

        var queryReq queryRequest
        d := json.NewDecoder(r.Body)
        if err := d.Decode(&queryReq); err != nil && err != io.EOF {
            http.Error(w, err.Error(), http.StatusBadRequest)
            return true
        } else if len(queryReq.Queries) == 0 {
            http.Error(w, "unexpected empty query", http.StatusBadRequest)
            return true
        }

        rdr := storage.NewReader()
        defer rdr.Close()

        resp := queryResponse{}
        resp.Data = []tableResp{}

        for _, query := range queryReq.Queries {
            var t tableResp
            t.Table = query.Table
            t.KVs = []kv{}

            prefix, err := TablePrefix(query.Table)
            if err != nil {
                http.Error(w, err.Error(), http.StatusBadRequest)
                return true
            }

            tr := rdr.NewTableReader(prefix)
            for _, key := range query.Keys {
                if err = tr.Get(key, func(v *string) {
                    t.KVs = append(t.KVs, kv{
                        Key:   key,
                        Value: v,
                    })
                }); err != nil {
                    http.Error(w, err.Error(), http.StatusInternalServerError)
                    return true
                }
            }

            resp.Data = append(resp.Data, t)
        }

        w.WriteHeader(http.StatusOK)
        w.Header().Set("Content-Type", "application/json; charset=utf-8")
        if err := json.NewEncoder(w).Encode(resp); err != nil {
            log.Warn("failed to encode response", zap.Error(err))
        }
        return true
    default:
        return false
    }
}

var (
    defaultPrefix    = []byte{0, 0}
    sqlDigestPrefix  = []byte{0, 1}
    planDigestPrefix = []byte{0, 2}
)

func TablePrefix(tableName string) ([]byte, error) {
    switch tableName {
    case "":
        return defaultPrefix, nil
    case "sql_digest":
        return sqlDigestPrefix, nil
    case "plan_digest":
        return planDigestPrefix, nil
    default:
        return nil, errors.New(fmt.Sprintf("no such table: %s", tableName))
    }
}
