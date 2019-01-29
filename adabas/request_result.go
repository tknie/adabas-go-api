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
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/SoftwareAG/adabas-go-api/adatypes"
)

type storeRecordTraverserStructure struct {
	record *Record
	helper *adatypes.BufferHelper
}

func createStoreRecordBuffer(adaValue adatypes.IAdaValue, x interface{}) (adatypes.TraverseResult, error) {
	record := x.(*storeRecordTraverserStructure)
	adatypes.Central.Log.Debugf("Store record buffer for %s current helper position is %d/%x",
		adaValue.Type().Name(), record.helper.Offset(), record.helper.Offset())
	err := adaValue.StoreBuffer(record.helper)
	adatypes.LogMultiLineString(adatypes.FormatByteBuffer("DATA: ", record.helper.Buffer()))
	return adatypes.Continue, err
}

func (record *Record) createRecordBuffer(helper *adatypes.BufferHelper) (err error) {
	adatypes.Central.Log.Debugf("Create record buffer")
	t := adatypes.TraverserValuesMethods{EnterFunction: createStoreRecordBuffer}
	stRecTraverser := &storeRecordTraverserStructure{record: record, helper: helper}
	_, err = record.traverse(t, stRecTraverser)
	adatypes.Central.Log.Debugf("Create record buffer done len=%d", len(helper.Buffer()))
	return
}

// Response contains the result information of the request
type Response struct {
	XMLName xml.Name  `xml:"Response" json:"-"`
	Values  []*Record `xml:"Records" json:"Records"`
}

// NrRecords number of records in the result
func (Response *Response) NrRecords() int {
	return len(Response.Values)
}

func prepareRecordDump(x interface{}, b interface{}) (adatypes.TraverseResult, error) {
	record := x.(*Record)
	var buffer *bytes.Buffer
	if b != nil {
		buffer = b.(*bytes.Buffer)
	}
	if record == nil {
		return adatypes.EndTraverser, adatypes.NewGenericError(25)
	}
	if record.Isn > 0 {
		if buffer == nil {
			fmt.Printf("Record Isn: %04d\n", record.Isn)
		} else {
			buffer.WriteString(fmt.Sprintf("Record Isn: %04d\n", record.Isn))
		}
	}
	if record.quantity > 0 {
		if buffer == nil {
			fmt.Printf("Record Quantity: %04d\n", record.quantity)
		} else {
			buffer.WriteString(fmt.Sprintf("Record Quantity: %04d\n", record.quantity))
		}
	}
	return adatypes.Continue, nil
}

func dumpRecord(adaValue adatypes.IAdaValue, x interface{}) (adatypes.TraverseResult, error) {
	y := strings.Repeat(" ", int(adaValue.Type().Level()))

	if x == nil {
		brackets := ""
		switch {
		case adaValue.PeriodIndex() > 0 && adaValue.MultipleIndex() > 0:
			brackets = fmt.Sprintf("[%02d,%02d]", adaValue.PeriodIndex(), adaValue.MultipleIndex())
		case adaValue.PeriodIndex() > 0:
			brackets = fmt.Sprintf("[%02d]", adaValue.PeriodIndex())
		case adaValue.MultipleIndex() > 0:
			brackets = fmt.Sprintf("[%02d]", adaValue.MultipleIndex())
		default:
		}

		if adaValue.Type().IsStructure() {
			structureValue := adaValue.(*adatypes.StructureValue)
			fmt.Println(y+" "+adaValue.Type().Name()+brackets+" = [", structureValue.NrElements(), "]")
		} else {
			fmt.Printf("%s %s%s = > %s <\n", y, adaValue.Type().Name(), brackets, adaValue.String())
		}
	} else {
		buffer := x.(*bytes.Buffer)
		buffer.WriteString(fmt.Sprintln(y, adaValue.Type().Name(), "= >", adaValue.String(), "<"))
	}
	return adatypes.Continue, nil
}

// DumpValues traverse through the tree of values calling a callback method
func (Response *Response) DumpValues() (err error) {
	fmt.Println("Dump all result values")
	t := adatypes.TraverserValuesMethods{PrepareFunction: prepareRecordDump, EnterFunction: dumpRecord}
	_, err = Response.TraverseValues(t, nil)
	return
}

// TraverseValues traverse through the tree of values calling a callback method
func (Response *Response) TraverseValues(t adatypes.TraverserValuesMethods, x interface{}) (ret adatypes.TraverseResult, err error) {
	adatypes.Central.Log.Debugf("Traverse result values")
	if Response.Values == nil {
		err = errors.New("no values")
		return
	}
	adatypes.Central.Log.Debugf("Go through records -> %d", len(Response.Values))
	var tr adatypes.TraverseResult
	for _, record := range Response.Values {
		if t.PrepareFunction != nil {
			tr, err = t.PrepareFunction(record, x)
			if err != nil || tr == adatypes.SkipStructure {
				return
			}
		}
		ret, err = record.traverse(t, x)
		if err != nil {
			return
		}
	}

	adatypes.Central.Log.Debugf("Ready traverse values")
	return
}

func (Response *Response) String() string {
	var buffer bytes.Buffer
	t := adatypes.TraverserValuesMethods{PrepareFunction: prepareRecordDump, EnterFunction: dumpRecord}
	Response.TraverseValues(t, &buffer)
	return buffer.String()
}

func traverseMarshalXML(adaValue adatypes.IAdaValue, x interface{}) (adatypes.TraverseResult, error) {
	enc := x.(*xml.Encoder)
	start := xml.StartElement{Name: xml.Name{Local: adaValue.Type().Name()}}
	enc.EncodeToken(start)
	if !adaValue.Type().IsStructure() {
		enc.EncodeToken(xml.CharData([]byte(adaValue.String())))
		enc.EncodeToken(start.End())
	}
	return adatypes.Continue, nil
}

func traverseMarshalXMLEnd(adaValue adatypes.IAdaValue, x interface{}) (adatypes.TraverseResult, error) {
	if adaValue.Type().IsStructure() {
		enc := x.(*xml.Encoder)
		end := xml.EndElement{Name: xml.Name{Local: adaValue.Type().Name()}}
		enc.EncodeToken(end)
	}
	return adatypes.Continue, nil
}

// MarshalXML provide XML
func (Response *Response) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	x := xml.StartElement{Name: xml.Name{Local: "Response"}}
	e.EncodeToken(x)
	tm := adatypes.TraverserValuesMethods{EnterFunction: traverseMarshalXML, LeaveFunction: traverseMarshalXMLEnd}
	adatypes.Central.Log.Debugf("Go through records -> %d", len(Response.Values))
	if Response.Values != nil {
		for _, record := range Response.Values {
			rec := xml.StartElement{Name: xml.Name{Local: "Record"}}
			rec.Attr = []xml.Attr{xml.Attr{Name: xml.Name{Local: "ISN"}, Value: strconv.Itoa(int(record.Isn))}}
			e.EncodeToken(rec)
			// e.EncodeToken(xml.Attr{Name: xml.Name{Local: "ISN"}, Value: strconv.Itoa(int(record.Isn))})
			record.traverse(tm, e)
			e.EncodeToken(rec.End())
		}
	}

	//	Response.TraverseValues(tm, e)
	// e.EncodeToken(xml.CharData([]byte("abc")))
	e.EncodeToken(x.End())
	return nil
}

func traverseMarshalXML2(adaValue adatypes.IAdaValue, x interface{}) (adatypes.TraverseResult, error) {
	enc := x.(*xml.Encoder)
	start := xml.StartElement{Name: xml.Name{Local: adaValue.Type().Name()}}
	enc.EncodeToken(start)
	if !adaValue.Type().IsStructure() {
		enc.EncodeToken(xml.CharData([]byte(adaValue.String())))
		enc.EncodeToken(start.End())
	}
	return adatypes.Continue, nil
}

func traverseMarshalXMLEnd2(adaValue adatypes.IAdaValue, x interface{}) (adatypes.TraverseResult, error) {
	if adaValue.Type().IsStructure() {
		enc := x.(*xml.Encoder)
		end := xml.EndElement{Name: xml.Name{Local: adaValue.Type().Name()}}
		enc.EncodeToken(end)
	}
	return adatypes.Continue, nil
}

// MarshalXML provide XML
func (record *Record) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	x := xml.StartElement{Name: xml.Name{Local: "Response"}}
	e.EncodeToken(x)
	tm := adatypes.TraverserValuesMethods{EnterFunction: traverseMarshalXML2, LeaveFunction: traverseMarshalXMLEnd2}
	rec := xml.StartElement{Name: xml.Name{Local: "Record"}}
	rec.Attr = []xml.Attr{xml.Attr{Name: xml.Name{Local: "ISN"}, Value: strconv.Itoa(int(record.Isn))}}
	e.EncodeToken(rec)
	record.traverse(tm, e)
	e.EncodeToken(rec.End())

	e.EncodeToken(x.End())
	return nil
}

type dataValue struct {
	Isn   adatypes.Isn `json:"ISN"`
	Value map[string]string
}

type request struct {
	Values         []*map[string]interface{} `json:"Records"`
	dataMap        *map[string]interface{}
	stack          *adatypes.Stack
	buffer         bytes.Buffer
	structureArray []interface{}
}

func evaluateValue(adaValue adatypes.IAdaValue) (interface{}, error) {
	switch adaValue.Type().Type() {
	case adatypes.FieldTypePacked, adatypes.FieldTypeUnpacked, adatypes.FieldTypeByte, adatypes.FieldTypeUByte,
		adatypes.FieldTypeUInt2, adatypes.FieldTypeInt2, adatypes.FieldTypeUInt4, adatypes.FieldTypeInt4,
		adatypes.FieldTypeUInt8, adatypes.FieldTypeInt8, adatypes.FieldTypeDouble, adatypes.FieldTypeFloat:
		v, err := adaValue.Int64()
		if err != nil {
			adatypes.Central.Log.Debugf("Error marshal JSON %s: %v", adaValue.Type().Name(), err)
			return adatypes.EndTraverser, err
		}
		return v, nil
	default:
		return strings.Trim(adaValue.String(), " "), nil
	}
	return nil, adatypes.NewGenericError(-1)
}

func traverseMarshalJSON(adaValue adatypes.IAdaValue, x interface{}) (adatypes.TraverseResult, error) {
	if !adaValue.Type().IsSpecialDescriptor() && !adaValue.Type().HasFlagSet(adatypes.FlagOptionMUGhost) {
		adatypes.Central.Log.Debugf("Marshal JSON level=%d %s -> type=%T MU ghost=%v", adaValue.Type().Level(),
			adaValue.Type().Name(), adaValue, adaValue.Type().HasFlagSet(adatypes.FlagOptionMUGhost))
		req := x.(*request)
		adatypes.Central.Log.Debugf("JSON stack size for %s->%d %T", adaValue.Type().Name(), req.stack.Size, adaValue)
		if adaValue.Type().IsStructure() {
			adatypes.Central.Log.Debugf("Structure Marshal JSON %s", adaValue.Type().Name())
			switch adaValue.Type().Type() {
			case adatypes.FieldTypeMultiplefield:
				sa := make([]interface{}, 0)
				sv := adaValue.(*adatypes.StructureValue)
				for _, values := range sv.Elements {
					for _, vi := range values.Values {
						v, err := evaluateValue(vi)
						if err != nil {
							return adatypes.EndTraverser, err
						}
						sa = append(sa, v)
					}
				}
				(*req.dataMap)[adaValue.Type().Name()] = sa
				adatypes.Central.Log.Debugf("Skip rest of MU Marshal JSON %s", adaValue.Type().Name())
				// return adatypes.SkipStructure, nil
			case adatypes.FieldTypePeriodGroup:
				// var sa []interface{}
				// fmt.Println(adaValue.Type().Name(), (*req.dataMap)[adaValue.Type().Name()])
				// debug.PrintStack()
				// req.stack.Push(req.dataMap)
				// dataMap := make(map[string]interface{})
				// oldMap := req.dataMap
				// req.dataMap = &dataMap
				// sa = append(sa, req.dataMap)
				// (*oldMap)[adaValue.Type().Name()] = sa
			default:
				req.stack.Push(req.dataMap)
				dataMap := make(map[string]interface{})
				oldMap := req.dataMap
				req.dataMap = &dataMap
				(*oldMap)[adaValue.Type().Name()] = req.dataMap
			}
		} else {
			v, err := evaluateValue(adaValue)
			if err != nil {
				adatypes.Central.Log.Debugf("JSON error %v", err)
				return adatypes.EndTraverser, err
			}
			(*req.dataMap)[adaValue.Type().Name()] = v
		}
	} else {
		adatypes.Central.Log.Debugf("Skip special descriptor Marshal JSON %s", adaValue.Type().Name())
	}
	return adatypes.Continue, nil
}

func traverseElementMarshalJSON(adaValue adatypes.IAdaValue, nr, max int, x interface{}) (adatypes.TraverseResult, error) {
	if adaValue.Type().Type() == adatypes.FieldTypePeriodGroup {
		req := x.(*request)
		if req.structureArray == nil {
			req.stack.Push(req.dataMap)
		}
		dataMap := make(map[string]interface{})
		req.dataMap = &dataMap
		req.structureArray = append(req.structureArray, req.dataMap)
	}
	return adatypes.Continue, nil
}

func traverseMarshalJSONEnd(adaValue adatypes.IAdaValue, x interface{}) (adatypes.TraverseResult, error) {
	if adaValue.Type().IsStructure() && adaValue.Type().Type() != adatypes.FieldTypeMultiplefield {
		sv := adaValue.(*adatypes.StructureValue)
		req := x.(*request)
		if adaValue.Type().Type() == adatypes.FieldTypePeriodGroup && len(sv.Elements) == 0 {
			(*req.dataMap)[adaValue.Type().Name()] = make([]interface{}, 0)
			adatypes.Central.Log.Debugf("JSON end skip for %s->%d", adaValue.Type().Name(), req.stack.Size)
			return adatypes.Continue, nil
		}
		dataMap, err := req.stack.Pop()
		if err != nil {
			adatypes.Central.Log.Debugf("JSON stack end %s error %v", adaValue.Type().Name(), err)
			return adatypes.EndTraverser, err
		}
		req.dataMap = dataMap.((*map[string]interface{}))
		if adaValue.Type().Type() == adatypes.FieldTypePeriodGroup {
			if req.structureArray == nil {
				(*req.dataMap)[adaValue.Type().Name()] = make([]interface{}, 0)
			} else {
				(*req.dataMap)[adaValue.Type().Name()] = req.structureArray
				req.structureArray = nil
			}
		}
		adatypes.Central.Log.Debugf("JSON end stack size for %s->%d", adaValue.Type().Name(), req.stack.Size)
	}
	return adatypes.Continue, nil
}

// MarshalJSON provide JSON
func (Response *Response) MarshalJSON() ([]byte, error) {
	req := &request{}
	adatypes.Central.Log.Debugf("Marshal JSON go through records -> %d", len(Response.Values))
	tm := adatypes.TraverserValuesMethods{EnterFunction: traverseMarshalJSON, LeaveFunction: traverseMarshalJSONEnd,
		ElementFunction: traverseElementMarshalJSON}
	req.stack = adatypes.NewStack()

	for _, record := range Response.Values {
		dataMap := make(map[string]interface{})
		req.dataMap = &dataMap
		req.Values = append(req.Values, req.dataMap)
		dataMap["ISN"] = record.Isn
		_, err := record.traverse(tm, req)
		if err != nil {
			adatypes.Central.Log.Debugf("Error creating JSON: %v", err)
			return nil, err
		}
	}
	return json.Marshal(req)
}

// Isn Search for record with given ISN
func (Response *Response) Isn(isn adatypes.Isn) *Record {
	for _, record := range Response.Values {
		if record.Isn == isn {
			return record
		}
	}
	return nil
}

type rrecord struct {
	stack       *adatypes.Stack
	buffer      bytes.Buffer
	hasElements bool
}

// MarshalJSON provide JSON
func (record *Record) MarshalJSON() ([]byte, error) {
	adatypes.Central.Log.Debugf("Marshal JSON record: %d", record.Isn)
	req := &request{}
	tm := adatypes.TraverserValuesMethods{EnterFunction: traverseMarshalJSON, LeaveFunction: traverseMarshalJSONEnd,
		ElementFunction: traverseElementMarshalJSON}
	req.stack = adatypes.NewStack()

	dataMap := make(map[string]interface{})
	req.dataMap = &dataMap
	req.Values = append(req.Values, req.dataMap)
	dataMap["ISN"] = record.Isn

	// Traverse record generating JSON
	_, err := record.traverse(tm, req)
	if err != nil {
		return nil, err
	}
	adatypes.Central.Log.Debugf("Go JSON response %v -> %s", err, req.buffer.String())

	return json.Marshal(req.dataMap)
}

// UnmarshalJSON parse JSON
// func (record *Record) UnmarshalJSON(b []byte) error {
// 	var stuff map[string]interface{}
// 	err := json.Unmarshal(b, &stuff)
// 	if err != nil {
// 		return err
// 	}
// 	if record.Value == nil {
// 		if record.definition.Values == nil {
// 			record.definition.CreateValues(false)
// 		}
// 		record.Value = record.definition.Values
// 	}
// 	for key, value := range stuff {
// 		fmt.Println("JSON:", key, "=", value)
// 		if key == "ISN" {
// 			isn, ierr := strconv.Atoi(value.(string))
// 			if ierr != nil {
// 				return ierr
// 			}
// 			record.Isn = adatypes.Isn(isn)
// 		} else {
// 			switch value.(type) {
// 			case map[string]interface{}:
// 				fmt.Println("JSON:", key, "=", value)
// 			default:
// 				err = record.SetValue(key, value)
// 				if err != nil {
// 					fmt.Println("Error setting key:", key)
// 					return err
// 				}
// 			}
// 		}
// 	}
// 	return nil
// }
