package comm

const (
	UPDFunc string = "upd"
	SubFunc string = "Sub"
	UnSubFunc string = "UnSub"
	UserQuotaTable string = "UserQuota"
	UserQuotaUpdateTable string = "UserQuotaUpdate"
	AccountQuotaTable string = "AccountQuota"
	AccountQuotaUpdateTable string = "AccountQuotaUpdate"
	RequestTable string = "requestxx"
	ResponseTable string = "response"
	MarketTable string = "Market"
	PositionTable string = "Position1"
	ProfitTable string = "Profit1"

)


type FuncTable struct {
	FuncName string
	TableName string
	Data	interface{}
}
