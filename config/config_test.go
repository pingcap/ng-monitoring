package config

import (
	"context"
	"os"
	"path"
	"runtime"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/database/docdb"
	"github.com/pingcap/ng-monitoring/utils"
	"github.com/pingcap/ng-monitoring/utils/testutil"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	config := GetDefaultConfig()
	config.setDefaultAdvertiseAddress()
	config.PD.Endpoints = []string{"10.0.1.8:2379"}
	require.NoError(t, config.valid())
	require.Equal(t, config.PD, PD{Endpoints: []string{"10.0.1.8:2379"}})
	require.Equal(t, config.Address, "0.0.0.0:12020")

	config = Config{}
	_, localFile, _, _ := runtime.Caller(0)
	configFile := path.Join(path.Dir(localFile), "config.toml.example")
	require.NoError(t, config.Load(configFile))
	require.Equal(t, config.Address, "0.0.0.0:12020")
	require.Equal(t, config.PD, PD{Endpoints: []string{"0.0.0.0:2379"}})
	require.Equal(t, config.Log, Log{Path: "log", Level: "INFO"})
	require.Equal(t, config.Storage, Storage{Path: "data", DocDBBackend: "sqlite", MetaRetentionSecs: 0})
}

func TestContinueProfilingConfig(t *testing.T) {
	cfg := ContinueProfilingConfig{}
	require.Equal(t, cfg.Valid(), false)
	cfg = ContinueProfilingConfig{
		ProfileSeconds:       0,
		IntervalSeconds:      60,
		TimeoutSeconds:       120,
		DataRetentionSeconds: 10000,
	}
	require.Equal(t, cfg.Valid(), false)
	cfg.ProfileSeconds = 61
	require.Equal(t, cfg.Valid(), false)
	cfg.ProfileSeconds = 41
	cfg.TimeoutSeconds = 40
	require.Equal(t, cfg.Valid(), false)
	cfg = ContinueProfilingConfig{
		ProfileSeconds:       10,
		IntervalSeconds:      60,
		TimeoutSeconds:       120,
		DataRetentionSeconds: 10000,
	}
	require.Equal(t, cfg.Valid(), true)
}

func TestConfigInit(t *testing.T) {
	cfgFileName := "test-cfg.toml"
	cfgData := `
address = "0.0.0.0:12020"
advertise-address = "10.0.1.8:12020"
[log]
path = "log"
level = "INFO"
[pd]
endpoints = ["10.0.1.8:2379"]
[storage]
path = "data"
[security]
ca-path = "ngm.ca"
cert-path = "ngm.cert"
key-path = "ngm.key"`
	err := os.WriteFile(cfgFileName, []byte(cfgData), 0666)
	require.NoError(t, err)
	defer os.Remove(cfgFileName)

	cfg, err := InitConfig(cfgFileName, func(config *Config) {})
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, "0.0.0.0:12020", cfg.Address)
	require.Equal(t, "10.0.1.8:12020", cfg.AdvertiseAddress)
	require.Equal(t, "log", cfg.Log.Path)
	require.Equal(t, "INFO", cfg.Log.Level)
	require.Len(t, cfg.PD.Endpoints, 1)
	require.Equal(t, "10.0.1.8:2379", cfg.PD.Endpoints[0])
	require.Equal(t, "data", cfg.Storage.Path)
	require.Equal(t, "ngm.ca", cfg.Security.SSLCA)
	require.Equal(t, "ngm.ca", cfg.Security.SSLCA)
	require.Equal(t, "ngm.cert", cfg.Security.SSLCert)
	require.Equal(t, "ngm.key", cfg.Security.SSLKey)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ReloadRoutine(ctx, cfgFileName)
	time.Sleep(time.Millisecond * 10)
	cfgData = `
address = "0.0.0.1:12020"
advertise-address = "10.0.1.9:12020"
[log]
path = "log1"
level = "ERROR"
[pd]
endpoints = ["10.0.1.8:2378", "10.0.1.9:2379"]
[storage]
path = "data1"`
	cfgSub := Subscribe()
	err = os.WriteFile(cfgFileName, []byte(cfgData), 0666)
	require.NoError(t, err)

	procutil.SelfSIGHUP()
	time.Sleep(time.Second)
	getCfg := <-cfgSub
	globalCfg := GetGlobalConfig()
	require.Equal(t, getCfg(), globalCfg)
	require.Equal(t, "0.0.0.0:12020", globalCfg.Address)
	require.Equal(t, "10.0.1.8:12020", globalCfg.AdvertiseAddress)
	require.Equal(t, "log", globalCfg.Log.Path)
	require.Equal(t, "INFO", globalCfg.Log.Level)
	require.Len(t, globalCfg.PD.Endpoints, 2)
	require.Equal(t, "10.0.1.8:2378", globalCfg.PD.Endpoints[0])
	require.Equal(t, "10.0.1.9:2379", globalCfg.PD.Endpoints[1])
	require.Equal(t, "data", globalCfg.Storage.Path)
	require.Equal(t, "ngm.ca", globalCfg.Security.SSLCA)
	require.Equal(t, "ngm.cert", globalCfg.Security.SSLCert)
	require.Equal(t, "ngm.key", globalCfg.Security.SSLKey)

	cfgData = ``
	err = os.WriteFile(cfgFileName, []byte(cfgData), 0666)
	require.NoError(t, err)
	procutil.SelfSIGHUP()
	// wait reload
	time.Sleep(time.Millisecond * 10)
	require.Equal(t, "0.0.0.0:12020", globalCfg.Address)
	require.Equal(t, "10.0.1.8:12020", globalCfg.AdvertiseAddress)
	require.Equal(t, "log", globalCfg.Log.Path)
	require.Equal(t, "INFO", globalCfg.Log.Level)
	require.Len(t, globalCfg.PD.Endpoints, 2)
	require.Equal(t, "10.0.1.8:2378", globalCfg.PD.Endpoints[0])
	require.Equal(t, "10.0.1.9:2379", globalCfg.PD.Endpoints[1])
	require.Equal(t, "data", globalCfg.Storage.Path)
	require.Equal(t, "ngm.ca", globalCfg.Security.SSLCA)
	require.Equal(t, "ngm.cert", globalCfg.Security.SSLCert)
	require.Equal(t, "ngm.key", globalCfg.Security.SSLKey)
}

func TestConfigValid(t *testing.T) {
	// Test PD valid
	pdCfg := PD{Endpoints: []string{}}
	require.Error(t, pdCfg.valid())
	require.Equal(t, pdCfg.valid().Error(), "unexpected empty pd endpoints, please specify at least one, e.g. --pd.endpoints \"127.0.0.1:2379\"")
	// Test Log
	logCfg := Log{}
	require.Equal(t, logCfg.valid().Error(), "unexpected empty log level")
	logCfg = Log{Path: "log", Level: "unknow"}
	require.Equal(t, logCfg.valid().Error(), "log level should be DEBUG, INFO, WARN or ERROR")
	logCfg = Log{Path: "log", Level: "INFO"}
	require.Equal(t, logCfg.valid(), nil)
	logCfg.InitDefaultLogger()
	// Test Storage
	storageCfg := Storage{}
	require.Equal(t, storageCfg.valid().Error(), "unexpected empty storage path")
	storageCfg = Storage{Path: "data"}
	require.Equal(t, storageCfg.valid(), nil)
	config := Config{}
	require.Equal(t, config.valid().Error(), "unexpected empty address")
	config = Config{PD: pdCfg, Log: logCfg, Storage: storageCfg}
	require.Equal(t, logCfg.valid(), nil)
}

func TestPDConfigEqual(t *testing.T) {
	// Test PD Equal
	pd1 := PD{Endpoints: []string{"10.0.1.8:2379", "10.0.1.9:2379"}}
	pd2 := PD{Endpoints: []string{"10.0.1.8:2378", "10.0.1.9:2378"}}
	require.False(t, pd1.Equal(pd2))
	pd2 = PD{Endpoints: []string{"10.0.1.8:2379"}}
	require.False(t, pd1.Equal(pd2))
	pd2 = PD{Endpoints: []string{"10.0.1.8:2379", "10.0.1.9:2379"}}
	require.True(t, pd1.Equal(pd2))
}

func TestTLS(t *testing.T) {
	cfg := Config{Security: Security{}}
	require.Equal(t, "http", cfg.GetHTTPScheme())
	caFile := "ca.crt"
	certFile := "ng.crt"
	pemFile := "ng.pem"
	err := os.WriteFile(caFile, []byte(`-----BEGIN CERTIFICATE-----
MIIDMDCCAhigAwIBAgIRAMe2loaMmf+umFBmQ9OKWBswDQYJKoZIhvcNAQELBQAw
ITEQMA4GA1UEChMHUGluZ0NBUDENMAsGA1UECxMEVGlVUDAgFw0yMTExMzAwNDQ5
MjZaGA8yMDcxMTExODA0NDkyNlowITEQMA4GA1UEChMHUGluZ0NBUDENMAsGA1UE
CxMEVGlVUDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALFAh2grK5ZC
zDhlp/cMA9Ph2WIZWIqMEVqU5J2l2i1+C+4hEZf7fwYKuA53x47ma2gCTiOM/9Tb
PsXJQDRdI76sDSW1D0Q5E5FIgPt4MJ2Ge5WNB0oqEp0EYJxcIOFjPB+euizFAJml
DvBARJBfvlXjfyvLnzzMJq17cZxGI+ZFlHBIwdz1W6kaBp4ufNc615NZGne8LE0H
9Xi9+5tQAj6Wd0G18Zm8heROsKZdoSNAEq51JJE8mXBw7hDiKcGxkTTjovU3wOGO
SIN/PgOOGLzQyP2UwtptzQxpnkschYd0RNwk7PAVtAQLDzqad29Pv4PIKNNW18In
ZzQiUlSJjl0CAwEAAaNhMF8wDgYDVR0PAQH/BAQDAgKEMB0GA1UdJQQWMBQGCCsG
AQUFBwMCBggrBgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MB0GA1UdDgQWBBQI1oUI
BkHltHykqaYYfu40x9QSWjANBgkqhkiG9w0BAQsFAAOCAQEATmMz6A/aCRdPSenF
ejObbH8m2L0jdxFf6xNXrKRI63CGD0xurniUSxAEYU7Ee6githkb9BsUSbmJWtwj
K3H6uNgsDj8BGiNAsI7YZo2cmo/z+mtP1tmevaP0GjnM3D6r6G/9XR8a7lkze91q
EaEOHyXhZh3gZlVeaiiDuWNpsDV45l2MtgosCyjmnClPlo5tFtEaVekI/uVW/eBJ
UqSQx401a2pJ4KP6DOW+2TEYjrqKVUBYNTgU7WjIac26YCnJcNDyNa8pEd4i6WVv
iyiUYFjTLgtegJgkOpDCKDyfj5l3OURQ2kemS1uv030uES6VaSvTfEuxtP27H1Aw
OFk8KA==
-----END CERTIFICATE-----`), 0666)
	require.NoError(t, err)

	err = os.WriteFile(certFile, []byte(`-----BEGIN CERTIFICATE-----
MIIDaTCCAlGgAwIBAgIRANt69n9jMGq75NskvBIbG98wDQYJKoZIhvcNAQELBQAw
ITEQMA4GA1UEChMHUGluZ0NBUDENMAsGA1UECxMEVGlVUDAeFw0yMTExMzAwNDUw
MjhaFw0zMTExMjgwNDUwMjhaMEkxEDAOBgNVBAoTB1BpbmdDQVAxIDALBgNVBAsT
BFRpVVAwEQYDVQQLEwpwcm9tZXRoZXVzMRMwEQYDVQQDEwpwcm9tZXRoZXVzMIIB
IjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0St0F7BIM9FFBXYyx2b/5yXG
SoaFWEgqcoHXw0Fj6qdmaO4ZOsQIkHQjA8deNHSjzkEvsZd4sTYaHbGe6qCsdu5Z
rVl1/zlIvN9juwcHAQPZfNKrfrBvE78IXnGc0xb1ibxwwGxjKahDMNTlFF/+Pb26
Sg5LVJL2EXlXSlF9pgURzW35iu3aU7SrKpaUBk4flnOkeUXbuZb/KmrYbJSL/wuz
VYa0idtaWAHM0+dIc75qnkYLiKr1TsvXmDZRLlzIegHQ9BxKSCDytk3xnYA5tKjA
DI2nlzrPGBgrNzJ8sEEB7bXQ0wSygf2pEgKtcQMLP2cuZtnw32KEXlebj2FWHQID
AQABo3QwcjAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwIGCCsG
AQUFBwMBMB8GA1UdIwQYMBaAFAjWhQgGQeW0fKSpphh+7jTH1BJaMCAGA1UdEQQZ
MBeCCWxvY2FsaG9zdIcEfwAAAYcECgABCzANBgkqhkiG9w0BAQsFAAOCAQEAlnQa
MGavPqk+iNHFn8LRW6cSUyTBtmCC47YJLge/tt2xqQb8jR+f28+zqBeo6NNi2mnb
tnSDHhj6qrcjjaIA5VL5op5+yWmNkqXR6yBjTqmdxLgWsLVsPFMXS2DtN8qZ0ukl
SxVNybD8NvWoCGdedNSEpQ+Gd3UxXK2gjPdZHPo3n0h9u8xfqKRNOQmg2N3GYSmp
fS6knrveE9Mmfgg/FmtcKi4WEd9+gydYzGwBiYRe1wIWjK1xuN3by00XEQR77wlb
mNGLnQMjURirpbLNCR9+IAsXk99VfCL0dVzZxo/pIz0fPqpwQfxj9ku8+yUJ4KqS
7195bvDsBG8Jnj/xiA==
-----END CERTIFICATE-----`), 0666)
	require.NoError(t, err)

	err = os.WriteFile(pemFile, []byte(`-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA0St0F7BIM9FFBXYyx2b/5yXGSoaFWEgqcoHXw0Fj6qdmaO4Z
OsQIkHQjA8deNHSjzkEvsZd4sTYaHbGe6qCsdu5ZrVl1/zlIvN9juwcHAQPZfNKr
frBvE78IXnGc0xb1ibxwwGxjKahDMNTlFF/+Pb26Sg5LVJL2EXlXSlF9pgURzW35
iu3aU7SrKpaUBk4flnOkeUXbuZb/KmrYbJSL/wuzVYa0idtaWAHM0+dIc75qnkYL
iKr1TsvXmDZRLlzIegHQ9BxKSCDytk3xnYA5tKjADI2nlzrPGBgrNzJ8sEEB7bXQ
0wSygf2pEgKtcQMLP2cuZtnw32KEXlebj2FWHQIDAQABAoIBAQCMlolMFrcg5Opg
Vmag8dDUeuZBVxMvGCo3lp//4+aVZHiH1Gjuv64F8ZlLQ+hEl5U130iANA/yBCwf
gzAOAXqJ4YAy7GtL5SPHltpAbeO+QekfZbXQzCOMgRzN5c0DcG4OarLaEr+/0xF+
M8nZHQAUXX5loh/ts21ip00NbaJnP89elgv3U5SupeyPLQZ7ke9xFYCTy5ujOf8Y
fPKpSlpT+vTR6PYeArUEuo9+mQ2JfdFt5h0YeJxiv6G41Msoeo4ucpQDsTf23UAm
FWEac57Or46+x/HWB3tbS0it9ce3roY0HmFKo7E0TL138fDjHup5ruECXBRZ9gMw
jlg7AAxBAoGBAPZdtIFhAG/6AuzKyGsqPvymb8wPbwIQfH3Y84xqIxvDiyyjSFBj
NAKkCS3ink8U9GjG85drd14wO6QOa9im/mFuBYzBvZYn8aWu7P/9Ar35BamsPOKK
7g91pl1WzAnAaEbg4NjdjCyxuff0e7Q9FdU7ak9vkA+cKZfXSPpSHAIxAoGBANlZ
Yz5gMyTvGST2QuJL0loLSKznvjk3uB3pdB32RHsyewqDLofpHVB3A7zEbVZR0Lbg
pBLAvqda72rqU4LQXZFPPugV3LWUAsn3jl92oUmrStt6eFIUuZUMMk49BkjUELK2
X585vzGqdCmxVYXILD9FKIHPBkkeG058h27rZ8utAoGBAMZq5auloiKNKrnm/88/
cQcuTK/+Zhs1h+4bQtt9x9TegkJrJxyHKSZPUo1ADNwINmgEg78Z8ENNeVtBuh39
MLbrU1Dv4G8EsJwN7BangQPbgXILo+WYmu6chGZ8N0xLSDB9gNloZTLB2NMYdmDN
Kb5YYeCkK1RHI0CFRONGKgShAoGADYZAZKs7w3qVR/WC5+3r4up81TV+YrUS4dma
/hpK3JehjF/pT0+0IUOmmeJnI03n/NkxnHEd6+/+odp+487vY5FYyrxBhZL2MXcU
BuCs3JaqC8otHn5npdyibLfjYji/6T7r6E6BlSeUHtwIBFEWX8F/6cPmEjqrXFDn
ZIGFbekCgYAjwwN0asGruVmTOipxPBccdfwZ8oz4/sBFIKhDf8T7JRc90SWgnidh
AHLPXeMGpx1QBP47VNlyClZhXMK7Yjkzmx2FIzCnYb4PG+Q+RPRse+UFNQvrPKii
Kv2FPAw7FhnHGC8nFubb4XtyPhFzNNv/J17pBsZNdGQuFawkyhpbCQ==
-----END RSA PRIVATE KEY-----`), 0644)
	require.NoError(t, err)
	defer func() {
		os.Remove(caFile)
		os.Remove(certFile)
		os.Remove(pemFile)
	}()
	cfg.Security = Security{
		SSLCA:   caFile,
		SSLCert: certFile,
		SSLKey:  pemFile,
	}
	require.Equal(t, "https", cfg.GetHTTPScheme())

	ccfg := cfg.Security.GetHTTPClientConfig()
	require.Equal(t, ccfg.TLSConfig.CAFile, caFile)
	require.Equal(t, ccfg.TLSConfig.CertFile, certFile)
	require.Equal(t, ccfg.TLSConfig.KeyFile, pemFile)
}

func TestConfigPersist(t *testing.T) {
	tmpDir, err := os.MkdirTemp(os.TempDir(), "ngm-test-.*")
	require.NoError(t, err)
	defer func() {
		err := os.RemoveAll(tmpDir)
		require.NoError(t, err)
	}()
	db, err := docdb.NewGenjiDBFromGenji(testutil.NewGenjiDB(t, tmpDir))
	require.NoError(t, err)
	defer db.Close()

	err = LoadConfigFromStorage(context.Background(), db)
	require.NoError(t, err)

	oldCfg := GetGlobalConfig()
	cfg := oldCfg
	cfg.ContinueProfiling.Enable = true
	cfg.ContinueProfiling.IntervalSeconds = 100
	StoreGlobalConfig(cfg)
	err = saveConfigIntoStorage(db)
	require.NoError(t, err)

	defCfg := GetDefaultConfig()
	StoreGlobalConfig(defCfg)
	err = LoadConfigFromStorage(context.Background(), db)
	require.NoError(t, err)
	curCfg := GetGlobalConfig()
	require.Equal(t, true, curCfg.ContinueProfiling.Enable)
	require.Equal(t, 100, curCfg.ContinueProfiling.IntervalSeconds)
}

func TestConfigInvalidAddress(t *testing.T) {
	cases := []struct {
		address          string
		advertiseAddress string
		err              string
	}{
		{"", "", "unexpected empty address"},
		{"0.0.0.0:0", "", "address 0.0.0.0:0 is invalid, err: port cannot be set to 0"},
		{"0.0.0.0:abc", "", "address 0.0.0.0:abc is invalid, err: strconv.Atoi: parsing \"abc\": invalid syntax"},
		{"0.0.0.0", "", "address 0.0.0.0 is invalid, err: address 0.0.0.0: missing port in address"},
		{"0.0.0.0:12020", "abc", "advertise-address abc is invalid, err: address abc: missing port in address"},
		{"0.0.0.0:12020", "0.0.0.0", "advertise-address 0.0.0.0 is invalid, err: address 0.0.0.0: missing port in address"},
		{"0.0.0.0:12020", "0.0.0.0:0", "advertise-address 0.0.0.0:0 is invalid, err: port cannot be set to 0"},
		{"0.0.0.0:12020", "0.0.0.0:abc", "advertise-address 0.0.0.0:abc is invalid, err: strconv.Atoi: parsing \"abc\": invalid syntax"},
		{"0.0.0.0:12020", "0.0.0.0:abc", "advertise-address 0.0.0.0:abc is invalid, err: strconv.Atoi: parsing \"abc\": invalid syntax"},
	}
	for _, c := range cases {
		cfg := defaultConfig
		cfg.Address = c.address
		cfg.AdvertiseAddress = c.advertiseAddress
		cfg.setDefaultAdvertiseAddress()
		err := cfg.valid()
		require.NotNil(t, err)
		require.Equal(t, c.err, err.Error())
	}
}

func TestConfigValidAddress(t *testing.T) {
	ip := utils.GetLocalIP()
	cases := []struct {
		address                  string
		advertiseAddress         string
		expectedAddress          string
		expectedAdvertiseAddress string
	}{
		{" 127.0.0.1:12020", "", "127.0.0.1:12020", "127.0.0.1:12020"},
		{" 0.0.0.0:12020 ", "", "0.0.0.0:12020", ip + ":12020"},
		{ip + ":12020 ", "", ip + ":12020", ip + ":12020"},
		{" 0.0.0.0:12020 ", "ngm-pod:12020", "0.0.0.0:12020", "ngm-pod:12020"},
	}
	for _, c := range cases {
		cfg, err := InitConfig("", func(cfg *Config) {
			cfg.Address = c.address
			cfg.AdvertiseAddress = c.advertiseAddress
		})
		require.NoError(t, err)
		require.Equal(t, c.expectedAdvertiseAddress, cfg.AdvertiseAddress)
		require.Equal(t, c.expectedAddress, cfg.Address)
	}
}
