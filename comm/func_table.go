package comm

const (
	UPDFunc string = "upd"
	ProfitUPDFunc string = "profit_upd"
	PositionUPDFunc string = "position_upd"
	UserQuotaUPDFunc string = "userquotaupd"
	AutoCloseRequestFunc string = "autoclose_upd"
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
	PositionTable string = "Position"
	ProfitTable string = "Profit"
	AutoCloseReuestTable string = "autocloserequest"
	AutoFlag string = "AUTO"
	AutoUser string = "AutoTrader"

)


type FuncTable struct {
	FuncName string
	TableName string
	Data	interface{}
}
