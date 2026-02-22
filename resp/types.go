package resp

import (
	"math"
	"strconv"
	"strings"
)

type DataType interface {
	String() string
	IsInitialized() bool
}

type Array struct {
	Len int
	Value []DataType
}

func NewArray() *Array {
	return &Array{Len: -2}
}

func (arr Array) String() string {
	var sb strings.Builder
	sb.WriteString("*" + strconv.Itoa(arr.Len) + "\r\n")
	for _, val := range arr.Value {
		sb.WriteString(val.String())
	}
	return sb.String()
}

func (arr Array) IsInitialized() bool {
	return len(arr.Value) == arr.Len && (arr.Len == 0 || arr.Value[arr.Len - 1].(*BulkString).IsInitialized())
}

type BulkString struct {
	Len int
	Value string
	ExpiresAt int64
}

func NewBulkString() *BulkString {
	return &BulkString{Len: -2, ExpiresAt: math.MaxInt64}
}

func NewBulkStringWithValue(value string) *BulkString {
	return &BulkString{Len: len(value), Value: value, ExpiresAt: math.MaxInt64}
}

func (bs BulkString) String() string {
	var sb strings.Builder
	sb.WriteString("$" + strconv.Itoa(bs.Len) + "\r\n")
	sb.WriteString(bs.Value)
	sb.WriteString("\r\n")
	return sb.String()
}

func (bs BulkString) IsInitialized() bool {
	return len(bs.Value) == bs.Len
}

type Integer struct {
	Value int64
}

func NewIntegerWithValue(value int64) *Integer {
	return &Integer{Value: value}
}

func (x Integer) String() string {
	var sb strings.Builder
	sb.WriteString(":")
	if x.Value < 0 {
		sb.WriteString("-")
	}
	sb.WriteString(strconv.FormatInt(x.Value, 10))
	sb.WriteString("\r\n")
	return sb.String()
}

func (x Integer) IsInitialized() bool {
	return true
}
