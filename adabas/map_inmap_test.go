package adabas

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefinitionInmap(t *testing.T) {
	initTestLogWithFile(t, "inmap.log")
	connection, cerr := NewConnection("acj;inmap=23,4")
	if !assert.NoError(t, cerr) {
		return
	}
	defer connection.Close()
	read, err := connection.CreateMapReadRequest(&Map{TypeID: 77})
	if !assert.NoError(t, err) {
		return
	}
	result, rErr := read.ReadLogicalWith("RN=EMPLOYEES-NAT-DDM")
	if !assert.NoError(t, rErr) {
		return
	}
	assert.Len(t, result.Data, 1)
	m := result.Data[0].(*Map)
	assert.Equal(t, uint8(77), m.TypeID)
	assert.Equal(t, "EMPLOYEES-NAT-DDM", m.Name)
	assert.Equal(t, "23(tcpip://host:0)", m.DataURL)
	validateResult(t, t.Name(), result)
}
