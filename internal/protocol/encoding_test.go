package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBytesToInt32(t *testing.T) {
	tests := []struct {
		name      string
		testCase  []byte
		expResult int32
	}{
		{
			name:      `266`,
			testCase:  []byte{0, 0, 1, 10},
			expResult: 266,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.name, func(t *testing.T) {
			newint := BytesToInt32(test.testCase)
			require.Equal(t, test.expResult, newint)
		})
	}
}

func TestBytesToInt16(t *testing.T) {
	tests := []struct {
		name      string
		testCase  []byte
		expResult int16
	}{
		{
			name:      `1029`,
			testCase:  []byte{4, 5},
			expResult: 1029,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.name, func(t *testing.T) {
			newint := BytesToInt16(test.testCase)
			require.Equal(t, test.expResult, newint)
		})
	}
}
