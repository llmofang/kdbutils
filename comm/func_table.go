package comm

const (
	UPDFunc string = "upd"
	UserQuotaUPDFunc string = "userquotaupd"
	SubFunc string = "Sub"
	UnSubFunc string = "UnSub"
	UserQuotaTable string = "UserQuota"
	UserQuotaUpdateTable string = "UserQuotaUpdate"
	AccountQuotaTable string = "AccountQuota"
	AccountQuotaUpdateTable string = "AccountQuotaUpdate"
	RequestTable string = "request"
	RequestFrontTable string = "requestxx"
	Response4WebTable string = "response"
	Response4NativeTable string = "response1"
	MarketTable string = "Market"
	PositionTable string = "Position1"
	ProfitTable string = "Profit1"

)


type FuncTable struct {
	FuncName string
	TableName string
	Data	interface{}
}
