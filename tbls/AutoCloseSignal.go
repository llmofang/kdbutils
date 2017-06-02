package tbls

import "time"

type AutoCloseSignal struct {
	Sym        string
	Time       time.Time
	Name       string
	Signaltype string
	Signal     float64
	Strength   float64
	Note       string
}
