package redists

import (
	"testing"
	"time"
)

func TestFilter_Arg(t *testing.T) {
	if got, want := FilterEqual("l", "v").Arg(), "l=v"; got != want {
		t.Errorf("FilterEqual() = %v, want %v", got, want)
	}
	if got, want := FilterEqual("l").Arg(), "l="; got != want {
		t.Errorf("FilterEqual() = %v, want %v", got, want)
	}
	if got, want := FilterEqual("l", "v1", "v2").Arg(), "l=(v1,v2)"; got != want {
		t.Errorf("FilterEqual() = %v, want %v", got, want)
	}
	if got, want := FilterNotEqual("l", "v").Arg(), "l!=v"; got != want {
		t.Errorf("FilterNotEqual() = %v, want %v", got, want)
	}
	if got, want := FilterNotEqual("l").Arg(), "l!="; got != want {
		t.Errorf("FilterNotEqual() = %v, want %v", got, want)
	}
	if got, want := FilterNotEqual("l", "v1", "v2").Arg(), "l!=(v1,v2)"; got != want {
		t.Errorf("FilterNotEqual() = %v, want %v", got, want)
	}
}

func Test_timestampArg(t *testing.T) {
	var ts Timestamp = time.UnixMilli(100)
	if got, want := timestampArg(ts), int64(100); got != want {
		t.Errorf("Arg() = %v, want %v", got, want)
	}
	ts = TSMin()
	if got, want := timestampArg(ts), "-"; got != want {
		t.Errorf("Arg() = %v, want %v", got, want)
	}
	ts = TSMax()
	if got, want := timestampArg(ts), "+"; got != want {
		t.Errorf("Arg() = %v, want %v", got, want)
	}
	ts = TSAuto()
	if got, want := timestampArg(ts), "*"; got != want {
		t.Errorf("Arg() = %v, want %v", got, want)
	}
}
