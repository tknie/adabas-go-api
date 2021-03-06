/*
* Copyright © 2020 Software AG, Darmstadt, Germany and/or its licensors
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
	"runtime"
	"testing"

	"github.com/SoftwareAG/adabas-go-api/adatypes"
	"github.com/stretchr/testify/assert"
)

type IncomeInMap struct {
	Salary   uint64   `adabas:"::AS"`
	Bonus    []uint64 `adabas:"::AT"`
	Currency string   `adabas:"::AR"`
	Summary  uint64   `adabas:":ignore"`
}

type EmployeesInMap struct {
	Index      uint64         `adabas:":isn"`
	ID         string         `adabas:":key:AA"`
	FullName   *FullNameInMap `adabas:"::AB"`
	Birth      uint64         `adabas:"::AH"`
	Department string         `adabas:"::AO"`
	Income     []*IncomeInMap `adabas:"::AQ"`
	Language   []string       `adabas:"::AZ"`
}

type FullNameInMap struct {
	FirstName  string `adabas:"::AC"`
	MiddleName string `adabas:"::AD"`
	Name       string `adabas:"::AE"`
}

func TestInlineMap(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping malloc count in short mode")
	}
	if runtime.GOARCH == "arm" {
		t.Skip("Not supported on this architecture")
		return
	}
	initTestLogWithFile(t, "inmap.log")

	connection, cerr := NewConnection("acj;inmap=23,11")
	if !assert.NoError(t, cerr) {
		return
	}
	defer connection.Close()
	adatypes.Central.Log.Debugf("Created connection : %#v", connection)
	request, err := connection.CreateMapReadRequest(&EmployeesInMap{})
	if !assert.NoError(t, err) {
		return
	}
	err = request.QueryFields("*")
	if !assert.NoError(t, err) {
		return
	}
	response, rerr := request.ReadISN(1024)
	if !assert.NoError(t, rerr) {
		return
	}
	response.DumpData()
	if assert.Len(t, response.Data, 1) {
		assert.Equal(t, "30021228", response.Data[0].(*EmployeesInMap).ID)
		assert.Equal(t, "JAMES               ", response.Data[0].(*EmployeesInMap).FullName.FirstName)
		assert.Equal(t, "SMEDLEY             ", response.Data[0].(*EmployeesInMap).FullName.Name)
		assert.Equal(t, "COMP02", response.Data[0].(*EmployeesInMap).Department)
	}
	response.DumpValues()
	assert.Len(t, response.Values, 0)
}

func TestInlineMapSearchAndOrder(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping malloc count in short mode")
	}
	if runtime.GOARCH == "arm" {
		t.Skip("Not supported on this architecture")
		return
	}
	initTestLogWithFile(t, "inmap.log")

	connection, cerr := NewConnection("acj;inmap=23,11")
	if !assert.NoError(t, cerr) {
		return
	}
	defer connection.Close()
	adatypes.Central.Log.Debugf("Created connection : %#v", connection)
	request, err := connection.CreateMapReadRequest(&EmployeesInMap{})
	if !assert.NoError(t, err) {
		return
	}
	err = request.QueryFields("*")
	if !assert.NoError(t, err) {
		return
	}
	response, rerr := request.SearchAndOrder("AA=50005600", "AE")
	if !assert.NoError(t, rerr) {
		return
	}
	response.DumpData()
	if assert.Len(t, response.Data, 1) {
		assert.Equal(t, "50005600", response.Data[0].(*EmployeesInMap).ID)
		assert.Equal(t, "HUMBERTO            ", response.Data[0].(*EmployeesInMap).FullName.FirstName)
		assert.Equal(t, "MORENO              ", response.Data[0].(*EmployeesInMap).FullName.Name)
		assert.Equal(t, "VENT07", response.Data[0].(*EmployeesInMap).Department)
	}
	response.DumpValues()
	assert.Len(t, response.Values, 0)
}

func TestInlineMapHistogram(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping malloc count in short mode")
	}
	if runtime.GOARCH == "arm" {
		t.Skip("Not supported on this architecture")
		return
	}
	initTestLogWithFile(t, "inmap.log")

	connection, cerr := NewConnection("acj;inmap=23,11")
	if !assert.NoError(t, cerr) {
		return
	}
	defer connection.Close()
	adatypes.Central.Log.Debugf("Created connection : %#v", connection)
	request, err := connection.CreateMapReadRequest(&EmployeesInMap{})
	if !assert.NoError(t, err) {
		return
	}
	err = request.QueryFields("*")
	if !assert.NoError(t, err) {
		return
	}
	response, rerr := request.HistogramWith("AO=VENT07")
	if !assert.NoError(t, rerr) {
		return
	}
	response.DumpData()
	assert.Len(t, response.Data, 0)
	response.DumpValues()
	if assert.Len(t, response.Values, 1) {
		assert.Equal(t, uint64(5), response.Values[0].Quantity)
	}
}

func TestInlineMapHistogramDesc(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping malloc count in short mode")
	}
	if runtime.GOARCH == "arm" {
		t.Skip("Not supported on this architecture")
		return
	}
	initTestLogWithFile(t, "inmap.log")

	connection, cerr := NewConnection("acj;inmap=23,11")
	if !assert.NoError(t, cerr) {
		return
	}
	defer connection.Close()
	adatypes.Central.Log.Debugf("Created connection : %#v", connection)
	request, err := connection.CreateMapReadRequest(&EmployeesInMap{})
	if !assert.NoError(t, err) {
		return
	}
	request.Limit = 4
	err = request.QueryFields("*")
	if !assert.NoError(t, err) {
		return
	}
	response, rerr := request.HistogramBy("AO")
	if !assert.NoError(t, rerr) {
		return
	}
	response.DumpData()
	assert.Len(t, response.Data, 0)
	response.DumpValues()
	if assert.Len(t, response.Values, 4) {
		assert.Equal(t, uint64(5), response.Values[0].Quantity)
		assert.Equal(t, "ADMA01", response.Values[0].HashFields["AO"].String())
		assert.Equal(t, uint64(35), response.Values[3].Quantity)
		assert.Equal(t, "COMP02", response.Values[3].HashFields["AO"].String())
	}
}

func TestInlineStoreMap(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping malloc count in short mode")
	}
	if runtime.GOARCH == "arm" {
		t.Skip("Not supported on this architecture")
		return
	}
	initTestLogWithFile(t, "inmap.log")

	clearAdabasFile(t, adabasModDBIDs, 16)

	fmt.Println("Starting inmap store ....")
	connection, cerr := NewConnection("acj;inmap=23,16")
	if !assert.NoError(t, cerr) {
		return
	}
	defer connection.Close()
	adatypes.Central.Log.Debugf("Created connection : %#v", connection)
	request, err := connection.CreateMapStoreRequest(&EmployeesInMap{})
	if !assert.NoError(t, err) {
		return
	}
	fmt.Println("Storing fields ....")
	err = request.StoreFields("*")
	if !assert.NoError(t, err) {
		return
	}
	e := &EmployeesInMap{FullName: &FullNameInMap{FirstName: "Anton", Name: "Skeleton", MiddleName: "Otto"}, Birth: 1234}
	fmt.Println("Storing record ....")
	rerr := request.StoreData(e)
	if !assert.NoError(t, rerr) {
		return
	}
	err = request.EndTransaction()
	if !assert.NoError(t, err) {
		return
	}
	checkContent(t, "inmapstore", "23", 16)
}

type EmployeeInMapPe struct {
	Name        string         `adabas:"::AE"`
	FirstName   string         `adabas:"::AD"`
	Isn         uint64         `adabas:":isn"`
	ID          string         `adabas:"::AA"`
	Income      []*InmapIncome `adabas:"::AQ"`
	AddressLine []string       `adabas:"::AI"`
	LeaveBooked []*InmapLeave  `adabas:"::AW"`
}

// Income income
type InmapIncome struct {
	Currency string `adabas:"::AR"`
	Salary   uint32 `adabas:"::AS"`
}

type InmapLeave struct {
	LeaveStart uint64 `adabas:"::AX"`
	LeaveEnd   uint64 `adabas:"::AY"`
}

func TestInlineStorePE(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping malloc count in short mode")
	}
	if runtime.GOARCH == "arm" {
		t.Skip("Not supported on this architecture")
		return
	}
	initTestLogWithFile(t, "inmap.log")

	clearAdabasFile(t, adabasModDBIDs, 16)

	fmt.Println("Starting inmap store ....")
	connection, cerr := NewConnection("acj;inmap=23,16")
	if !assert.NoError(t, cerr) {
		return
	}
	defer connection.Close()
	adatypes.Central.Log.Debugf("Created connection : %#v", connection)
	request, err := connection.CreateMapStoreRequest(&EmployeeInMapPe{})
	if !assert.NoError(t, err) {
		return
	}
	fmt.Println("Storing fields ....")
	err = request.StoreFields("*")
	if !assert.NoError(t, err) {
		return
	}
	j := &EmployeeInMapPe{Name: "XXX", ID: "fdlldnfg", LeaveBooked: []*InmapLeave{{LeaveStart: 3434, LeaveEnd: 232323}}}
	fmt.Println("Storing record ....")
	rerr := request.StoreData(j)
	if !assert.NoError(t, rerr) {
		return
	}
	err = request.EndTransaction()
	if !assert.NoError(t, err) {
		return
	}
	checkContent(t, "inmapstorepe", "23", 16)
}

func TestInlineStorePEMU(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping malloc count in short mode")
	}
	if runtime.GOARCH == "arm" {
		t.Skip("Not supported on this architecture")
		return
	}
	initTestLogWithFile(t, "inmap.log")

	clearAdabasFile(t, adabasModDBIDs, 16)

	fmt.Println("Starting inmap store ....")
	connection, cerr := NewConnection("acj;inmap=23,16")
	if !assert.NoError(t, cerr) {
		return
	}
	defer connection.Close()
	adatypes.Central.Log.Debugf("Created connection : %#v", connection)
	request, err := connection.CreateMapStoreRequest(&EmployeeInMapPe{})
	if !assert.NoError(t, err) {
		return
	}
	fmt.Println("Storing fields ....")
	err = request.StoreFields("*")
	if !assert.NoError(t, err) {
		return
	}
	j := &EmployeeInMapPe{Name: "XXX", ID: "fdlldnfg", Income: []*InmapIncome{{Currency: "ABB", Salary: 121324}}}
	fmt.Println("Storing record ....")
	rerr := request.StoreData(j)
	if !assert.NoError(t, rerr) {
		return
	}
	err = request.EndTransaction()
	if !assert.NoError(t, err) {
		return
	}
	checkContent(t, "inmapstorepemu", "23", 16)
}
