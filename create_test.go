package redists

import (
	"reflect"
	"testing"
	"time"
)

func TestCmdCreate(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		cmd := newCmdCreate("key:any")
		if got, want := cmd.Name(), "TS.CREATE"; got != want {
			t.Errorf("Name() = %v, want %v", got, want)
		}
		if got, want := cmd.Args(), []interface{}{"key:any"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("retention", func(t *testing.T) {
		cmd := newCmdCreate("key:any")
		CreateWithRetention(time.Second)(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", "RETENTION", int64(1000)}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("encoding", func(t *testing.T) {
		cmd := newCmdCreate("key:any")
		CreateWithEncoding(EncodingCompressed)(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", "ENCODING", "COMPRESSED"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("chunk size", func(t *testing.T) {
		cmd := newCmdCreate("key:any")
		CreateWithChunkSize(8)(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", "CHUNK_SIZE", 8}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("duplicate policy", func(t *testing.T) {
		cmd := newCmdCreate("key:any")
		CreateWithDuplicatePolicy(DuplicatePolicyBlock)(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", "DUPLICATE_POLICY", "BLOCK"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("labels", func(t *testing.T) {
		cmd := newCmdCreate("key:any")
		CreateWithLabels(Labels{
			"label:any":   "value:any",
			"label:other": "value:other",
		})(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", "LABELS", "label:any", "value:any", "label:other", "value:other"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("args order", func(t *testing.T) {
		cmd := newCmdCreate("key:any")
		want := []interface{}{
			"key:any",
			"RETENTION", int64(1000),
			"ENCODING", "COMPRESSED",
			"CHUNK_SIZE", 8,
			"DUPLICATE_POLICY", "BLOCK",
			"LABELS", "label:any", "value:any",
		}
		CreateWithRetention(time.Second)(cmd)
		CreateWithEncoding(EncodingCompressed)(cmd)
		CreateWithChunkSize(8)(cmd)
		CreateWithDuplicatePolicy(DuplicatePolicyBlock)(cmd)
		CreateWithLabels(Labels{"label:any": "value:any"})(cmd)
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
		cmd = newCmdCreate("key:any")
		CreateWithLabels(Labels{"label:any": "value:any"})(cmd)
		CreateWithDuplicatePolicy(DuplicatePolicyBlock)(cmd)
		CreateWithChunkSize(8)(cmd)
		CreateWithEncoding(EncodingCompressed)(cmd)
		CreateWithRetention(time.Second)(cmd)
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
}
