/*
* Copyright © 2018-2022 Software AG, Darmstadt, Germany and/or its licensors
*
* SPDX-License-Identifier: Apache-2.0
*
*   Licensed under the Apache License, Version 2.0 (the "License");
*   you may not use this file except in compliance with the License.
*   You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*   Unless required by applicable law or agreed to in writing, software
*   distributed under the License is distributed on an "AS IS" BASIS,
*   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*   See the License for the specific language governing permissions and
*   limitations under the License.
*
 */

package adabas

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tknie/adabas-go-api/adatypes"
)

const maxMaps = 20

func TestMapRepositoryReadAll(t *testing.T) {
	initTestLogWithFile(t, "map_repositories.log")

	adatypes.Central.Log.Infof("TEST: %s", t.Name())
	adabas, _ := NewAdabas(24)
	defer adabas.Close()
	mr := NewMapRepository(adabas, 4)
	adabasMaps, err := mr.LoadAllMaps(adabas)
	assert.NoError(t, err)
	assert.NotNil(t, adabasMaps)
	assert.NotEqual(t, 0, len(adabasMaps))
	for _, m := range adabasMaps {
		fmt.Println(m.Name)
	}
}

func TestMapRepositoryRead(t *testing.T) {
	initTestLogWithFile(t, "map_repositories.log")

	adatypes.Central.Log.Infof("TEST: %s", t.Name())
	adabas, _ := NewAdabas(23)
	defer adabas.Close()
	mr := NewMapRepository(adabas, 4)
	employeeMap, serr := mr.SearchMap(adabas, "EMPLOYEES-NAT-DDM")
	assert.NotNil(t, employeeMap)
	assert.NoError(t, serr)
	// fmt.Println(">", employeeMap.String())
	// adabasMaps, err := mr.LoadAllMaps(adabas)
	// assert.NoError(t, err)
	// assert.NotNil(t, adabasMaps)
	// assert.NotEqual(t, 0, len(adabasMaps))
	// for _, m := range adabasMaps {
	// 	if m.Name == "EMPLOYEES-NAT-DDM" {
	// 		employeeMap = m
	// 	}
	// }
	// fmt.Println(">", employeeMap.String())
	x := employeeMap.fieldMap["AA"]
	assert.NotNil(t, x)
	// fmt.Printf("%#v", x)
}

func BenchmarkReadMapOld(b *testing.B) {
	initLogWithFile("map_repositories_bench.log")

	adatypes.Central.Log.Infof("TEST: %s", b.Name())
	adabas, _ := NewAdabas(23)
	defer adabas.Close()
	mr := NewMapRepository(adabas, 4)
	baseMap, err := mr.SearchMap(adabas, "EMPLOYEES-NAT-DDM")
	if !assert.NoError(b, err) {
		return
	}
	createMaps(b, baseMap)
	_, err = mr.LoadAllMaps(adabas)
	if !assert.NoError(b, err) {
		return
	}
	b.ResetTimer()
	adabas2, _ := NewAdabas(23)
	defer adabas2.Close()
	for i := 0; i < b.N; i++ {
		index := b.N % maxMaps
		name := "Test" + strconv.Itoa(index)
		m, err2 := mr.readAdabasMap(adabas2, name)
		if !assert.NoError(b, err2) {
			return
		}
		assert.Equal(b, name, m.Name)
	}
	defer cleanup(b, baseMap)
}

func createMaps(b *testing.B, baseMap *Map) {
	cleanup(b, baseMap)
	for i := 0; i < maxMaps; i++ {
		baseMap.Name = "Test" + strconv.Itoa(i)
		err := baseMap.Store()
		if !assert.NoError(b, err) {
			return
		}
	}
}

func cleanup(b *testing.B, baseMap *Map) {
	for i := 0; i < maxMaps; i++ {
		baseMap.Name = "Test" + strconv.Itoa(i)
		baseMap.Delete()
	}
}

func readMap(t *testing.B, read *ReadRequest, name string) *Map {
	result, rErr := read.ReadLogicalWith("RN=" + name)
	if !assert.NoError(t, rErr) {
		return nil
	}
	return result.Data[0].(*Map)

}

func BenchmarkReadMapNew(b *testing.B) {
	initLogWithFile("map_repositories_bench.log")

	adatypes.Central.Log.Infof("TEST: %s", b.Name())

	connection, cerr := NewConnection("acj;inmap=23,4")
	if !assert.NoError(b, cerr) {
		return
	}
	defer connection.Close()

	read, err := connection.CreateMapReadRequest(&Map{TypeID: 77})
	if !assert.NoError(b, err) {
		return
	}
	baseMap := readMap(b, read, "EMPLOYEES-NAT-DDM")
	if !assert.NotNil(b, baseMap) {
		return
	}

	createMaps(b, baseMap)

	mr := NewMapRepository(connection.adabasToData, 4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := b.N % maxMaps
		name := "Test" + strconv.Itoa(index)
		m := readMap(b, read, name)
		if !assert.NotNil(b, m) {
			return
		}
		assert.Equal(b, name, m.Name)
		mr.AddMapToCache(name, m)
	}
	defer cleanup(b, baseMap)
}
