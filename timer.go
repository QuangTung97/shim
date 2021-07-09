package shim

import "time"

type simpleTimer struct {
	d     time.Duration
	timer *time.Timer
}

func newTimer(d time.Duration, callback func()) Timer {
	return &simpleTimer{
		d:     d,
		timer: time.AfterFunc(d, callback),
	}
}

func (t *simpleTimer) Reset() {
	t.timer.Reset(t.d)
}

func (t *simpleTimer) Stop() {
	t.timer.Stop()
}
