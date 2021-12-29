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

func TestTimestamp_Min(t1 *testing.T) {
	tests := []struct {
		name string
		ts   Timestamp
		want bool
	}{
		{name: "normal", ts: TS(time.UnixMilli(100)), want: false},
		{name: "min", ts: TSMin(), want: true},
		{name: "max", ts: TSMax(), want: false},
		{name: "auto", ts: TSAuto(), want: false},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			if got := tt.ts.Min(); got != tt.want {
				t1.Errorf("Min() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimestamp_Max(t1 *testing.T) {
	tests := []struct {
		name string
		ts   Timestamp
		want bool
	}{
		{name: "normal", ts: TS(time.UnixMilli(100)), want: false},
		{name: "min", ts: TSMin(), want: false},
		{name: "max", ts: TSMax(), want: true},
		{name: "auto", ts: TSAuto(), want: false},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			if got := tt.ts.Max(); got != tt.want {
				t1.Errorf("Max() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimestamp_Auto(t1 *testing.T) {
	tests := []struct {
		name string
		ts   Timestamp
		want bool
	}{
		{name: "normal", ts: TS(time.UnixMilli(100)), want: false},
		{name: "min", ts: TSMin(), want: false},
		{name: "max", ts: TSMax(), want: false},
		{name: "auto", ts: TSAuto(), want: true},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			if got := tt.ts.Auto(); got != tt.want {
				t1.Errorf("Auto() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimestamp_Arg(t *testing.T) {
	ts := TS(time.UnixMilli(100))
	if got, want := ts.Arg(), int64(100); got != want {
		t.Errorf("Arg() = %v, want %v", got, want)
	}
	ts = TSMin()
	if got, want := ts.Arg(), "-"; got != want {
		t.Errorf("Arg() = %v, want %v", got, want)
	}
	ts = TSMax()
	if got, want := ts.Arg(), "+"; got != want {
		t.Errorf("Arg() = %v, want %v", got, want)
	}
	ts = TSAuto()
	if got, want := ts.Arg(), "*"; got != want {
		t.Errorf("Arg() = %v, want %v", got, want)
	}
}
