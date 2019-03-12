/*
* Copyright © 2018-2019 Software AG, Darmstadt, Germany and/or its licensors
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
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestMapRepository(t *testing.T) {
	f := initTestLogWithFile(t, "map_repositories.log")
	defer f.Close()

	log.Infof("TEST: %s", t.Name())
	ada, _ := NewAdabas(24)
	defer ada.Close()
	AddGlobalMapRepository(ada, 4)
	defer DelGlobalMapRepository(ada, 4)
	adabas, _ := NewAdabas(1)
	defer adabas.Close()
	adabasMap, err := SearchMapRepository(adabas, "EMPLOYEES-NAT-DDM")
	assert.NoError(t, err)
	assert.NotNil(t, adabasMap)

}

func TestGlobalMapRepository(t *testing.T) {
	f := initTestLogWithFile(t, "map_repositories.log")
	defer f.Close()

	log.Infof("TEST: %s", t.Name())
	ada, _ := NewAdabas(23)
	defer ada.Close()
	AddGlobalMapRepository(ada, 4)
	defer DelGlobalMapRepository(ada, 4)
	ada.SetDbid(24)
	AddGlobalMapRepository(ada, 4)
	defer DelGlobalMapRepository(ada, 4)

	ada2, _ := NewAdabas(1)
	defer ada2.Close()
	adabasMaps, err := GloablMaps(ada2)
	assert.NoError(t, err)
	assert.NotNil(t, adabasMaps)
	for _, m := range adabasMaps {
		fmt.Printf("%s -> %d\n", m.Name, m.Isn)
	}

}

func TestMapRepositoryReadAll(t *testing.T) {
	f := initTestLogWithFile(t, "map_repositories.log")
	defer f.Close()

	log.Infof("TEST: %s", t.Name())
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

func TestGlobalMapConnectionString(t *testing.T) {
	f := initTestLogWithFile(t, "map_repositories.log")
	defer f.Close()

	log.Infof("TEST: %s", t.Name())
	ada, _ := NewAdabas(24)
	defer ada.Close()
	AddGlobalMapRepository(ada, 4)
	defer DelGlobalMapRepository(ada, 4)

	connection, cerr := NewConnection("acj;map=EMPLOYEES")
	if !assert.NoError(t, cerr) {
		return
	}
	defer connection.Close()

	request, rerr := connection.CreateReadRequest()
	if !assert.NoError(t, rerr) {
		return
	}
	request.QueryFields("name,personnel-id")
	result, err := request.ReadLogicalWith("personnel-id=[11100301:11100303]")
	if !assert.NoError(t, err) {
		return
	}
	result.DumpValues()
}

func TestGlobalMapConnectionDirect(t *testing.T) {
	f := initTestLogWithFile(t, "map_repositories.log")
	defer f.Close()

	log.Infof("TEST: %s", t.Name())
	ada, _ := NewAdabas(24)
	defer ada.Close()
	AddGlobalMapRepository(ada, 4)
	defer DelGlobalMapRepository(ada, 4)

	connection, cerr := NewConnection("acj;map")
	if !assert.NoError(t, cerr) {
		return
	}
	defer connection.Close()

	request, rerr := connection.CreateMapReadRequest("EMPLOYEES")
	if !assert.NoError(t, rerr) {
		return
	}
	request.QueryFields("name,personnel-id")
	result, err := request.ReadLogicalWith("personnel-id=[11100301:11100303]")
	if !assert.NoError(t, err) {
		return
	}
	result.DumpValues()
}
