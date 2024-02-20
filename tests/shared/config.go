package sharedtest

var (
	MySQLDSN    = "strongforce:strongforce@tcp(127.0.0.1:65001)/strongforce?charset=utf8mb4&multiStatements=true&parseTime=true"
	PostgresDSN = "postgresql://strongforce:strongforce@127.0.0.1:65002/strongforce?sslmode=disable"
	NATS        = "127.0.0.1:23945"
)
