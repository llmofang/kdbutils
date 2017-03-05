package comm

const (
	UPDFunc string = "upsert"
	SubFunc string = "Sub"
	UnSubFunc string = "UnSub"
	UserQuotaTable string = "UserQuota"
	AccountQuotaTable string = "AccountQuota"
	RequestTable string = "request"
	PositionTable string = "Position1"
	ProfitTable string = "Profit1"

)


type FuncTable struct {
	FuncName string
	TableName string
	Data	interface{}
}
