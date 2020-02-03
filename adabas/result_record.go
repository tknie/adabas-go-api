/*
* Copyright © 2018-2020 Software AG, Darmstadt, Germany and/or its licensors
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
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/SoftwareAG/adabas-go-api/adatypes"
)

// Record one result record of the result list received by
// record list or in the stream callback.
//
// To extract the values in the record you might request the
// value using the SearchValue() methods. Alternatively you
// might use the Traverse() callback method to call a method
// for each Adabas field in the tree. The tree includes group
// nodes of the Adabas record.
type Record struct {
	Isn        adatypes.Isn `xml:"Isn,attr"`
	Quantity   uint64       `xml:"Quantity,attr"`
	Value      []adatypes.IAdaValue
	HashFields map[string]adatypes.IAdaValue `xml:"-" json:"-"`
	fields     map[string]*queryField
	definition *adatypes.Definition
	adabasMap  *Map
}

func traverseHashValues(adaValue adatypes.IAdaValue, x interface{}) (adatypes.TraverseResult, error) {
	record := x.(*Record)
	//if _, ok := record.HashFields[adaValue.Type().Name()]; !ok {
	if !adaValue.Type().HasFlagSet(adatypes.FlagOptionMUGhost) {
		adatypes.Central.Log.Debugf("Add hash to %s", adaValue.Type().Name())
		record.HashFields[adaValue.Type().Name()] = adaValue
	}
	//}

	return adatypes.Continue, nil
}

// NewRecord new result record
func NewRecord(definition *adatypes.Definition) (*Record, error) {
	if definition == nil {
		adatypes.Central.Log.Debugf("Definition values empty")
		return nil, adatypes.NewGenericError(69)
	}
	if definition.Values == nil {
		err := definition.CreateValues(false)
		if err != nil {
			return nil, err
		}
	}
	record := &Record{Value: definition.Values, definition: definition}
	definition.Values = nil
	record.HashFields = make(map[string]adatypes.IAdaValue)
	t := adatypes.TraverserValuesMethods{EnterFunction: traverseHashValues}
	record.Traverse(t, record)
	return record, nil
}

// NewRecordIsn new result record with ISN or ISN quantity
func NewRecordIsn(isn adatypes.Isn, isnQuantity uint64, definition *adatypes.Definition) (*Record, error) {
	record, err := NewRecord(definition)
	if err != nil {
		return nil, err
	}
	record.Isn = isn
	record.Quantity = isnQuantity
	adatypes.Central.Log.Debugf("New record with ISN=%d and ISN quantity=%d", isn, isnQuantity)

	return record, nil
}

func recordValuesTraverser(adaValue adatypes.IAdaValue, x interface{}) (adatypes.TraverseResult, error) {
	buffer := x.(*bytes.Buffer)
	buffer.WriteString(fmt.Sprintf(" %s=%#v\n", adaValue.Type().Name(), adaValue.String()))
	return adatypes.Continue, nil
}

func (record *Record) createRecordBuffer(helper *adatypes.BufferHelper, option *adatypes.BufferOption) (err error) {
	adatypes.Central.Log.Debugf("Create store record buffer")
	t := adatypes.TraverserValuesMethods{EnterFunction: createStoreRecordBuffer}
	stRecTraverser := &storeRecordTraverserStructure{record: record, helper: helper, option: option}
	_, err = record.Traverse(t, stRecTraverser)
	adatypes.Central.Log.Debugf("Create record buffer done len=%d", len(helper.Buffer()))
	return
}

func (record *Record) String() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("ISN=%d quantity=%d\n", record.Isn, record.Quantity))
	t := adatypes.TraverserValuesMethods{EnterFunction: recordValuesTraverser}
	record.Traverse(t, &buffer)
	// for _, v := range record.Value {
	// 	buffer.WriteString(fmt.Sprintf("value=%#v\n", v))
	// }
	return buffer.String()
}

// Traverse step/traverse through all record entries and call methods
func (record *Record) Traverse(t adatypes.TraverserValuesMethods, x interface{}) (ret adatypes.TraverseResult, err error) {
	if record == nil {
		return adatypes.EndTraverser, adatypes.NewGenericError(33)
	}

	for _, value := range record.Value {
		adatypes.Central.Log.Debugf("Go through value %s %d", value.Type().Name(), value.Type().Type())
		if t.EnterFunction != nil {
			adatypes.Central.Log.Debugf("Enter field=%s Type=%d", value.Type().Name(), value.Type().Type())
			ret, err = t.EnterFunction(value, x)
			if err != nil {
				return
			}
			if ret == adatypes.SkipStructure {
				continue
			}
		}
		if value.Type().IsStructure() {
			adatypes.Central.Log.Debugf("Go through structure %s %d", value.Type().Name(), value.Type().Type())
			ret, err = value.(adatypes.StructureValueTraverser).Traverse(t, x)
			if err != nil || ret == adatypes.EndTraverser {
				return
			}
		}
		if t.LeaveFunction != nil {
			adatypes.Central.Log.Debugf("Leave %s %d", value.Type().Name(), value.Type().Type())
			ret, err = t.LeaveFunction(value, x)
			if err != nil || ret == adatypes.EndTraverser {
				return
			}
		}
	}
	adatypes.Central.Log.Debugf("Traverse ended")
	return
}

// DumpValues traverse through the tree of values calling a callback method
func (record *Record) DumpValues() {
	fmt.Println("Dump all record values")
	var buffer bytes.Buffer
	t := adatypes.TraverserValuesMethods{PrepareFunction: prepareRecordDump,
		EnterFunction: traverseDumpRecord}
	record.Traverse(t, &buffer)
	fmt.Printf("%s", buffer.String())
}

func (record *Record) searchValue(field string) (adatypes.IAdaValue, bool) {
	if adaValue, ok := record.HashFields[field]; ok {
		return adaValue, true
	}
	return nil, false
}

// SetValue set the value for a specific field
func (record *Record) SetValue(field string, value interface{}) (err error) {
	adatypes.Central.Log.Debugf("Set value %s", field)
	if strings.ContainsRune(field, '[') {
		i := strings.IndexRune(field, '[')
		c := strings.IndexRune(field[i:], ',')
		e := strings.IndexRune(field, ']')
		if c > 0 {
			eField := field[:i]
			index1, xerr := strconv.Atoi(field[i+1 : i+c])
			if xerr != nil {
				return xerr
			}
			index2, xerr := strconv.Atoi(field[i+c+1 : e])
			if xerr != nil {
				return xerr
			}
			return record.SetValueWithIndex(eField, []uint32{uint32(index1), uint32(index2)}, value)
		}

		index, xerr := strconv.Atoi(field[i+1 : e])
		if xerr != nil {
			return xerr
		}
		eField := field[:i]
		f := field[e+1:]
		i = strings.IndexRune(f, '[')
		if i == -1 {
			return record.SetValueWithIndex(eField, []uint32{uint32(index)}, value)
		}
		e = strings.IndexRune(f, ']')
		muindex, merr := strconv.Atoi(f[i+1 : e])
		if merr != nil {
			return merr
		}

		return record.SetValueWithIndex(eField, []uint32{uint32(index), uint32(muindex)}, value)
	}
	if adaValue, ok := record.searchValue(field); ok {
		err = adaValue.SetValue(value)
		adatypes.Central.Log.Debugf("Set %s [%T] value err=%v", field, adaValue, err)
		// TODO check if the field which is not found and stored should be checked
	} else {
		adatypes.Central.Log.Debugf("Field %s not found %v", field, adaValue)
		err = adatypes.NewGenericError(28, field)
	}
	return
}

// SetValueWithIndex Add value to an node element
func (record *Record) SetValueWithIndex(name string, index []uint32, x interface{}) error {
	// TODO why specific?
	record.definition.Values = record.Value
	adatypes.Central.Log.Debugf("Record value : %#v", record.Value)
	return record.definition.SetValueWithIndex(name, index, x)
}

func extractIndex(name string) []uint32 {
	var index []uint32
	var re = regexp.MustCompile(`(?m)(\w+(\[(\d+),?(\d+)?\])?)`)
	for _, s := range re.FindAllStringSubmatch(name, -1) {
		v, err := strconv.Atoi(s[3])
		if err != nil {
			return index
		}
		index = append(index, uint32(v))
		if s[4] != "" {
			v, err = strconv.Atoi(s[4])
			if err != nil {
				return index
			}
			index = append(index, uint32(v))
		}
	}
	return index
}

// SearchValue search value in the tree
func (record *Record) SearchValue(parameter ...interface{}) (adatypes.IAdaValue, error) {
	name := parameter[0].(string)
	var index []uint32
	if len(parameter) > 1 {
		index = parameter[1].([]uint32)
	} else {
		if strings.ContainsRune(name, '[') {
			index = extractIndex(name)
			name = name[:strings.IndexRune(name, '[')]
		} else {
			index = []uint32{0, 0}
		}
	}

	if v, ok := record.HashFields[name]; ok && !v.Type().HasFlagSet(adatypes.FlagOptionPE) {
		return v, nil
	}
	adatypes.Central.Log.Debugf("Search %s index: %#v", name, index)
	return record.SearchValueIndex(name, index)
}

// SearchValueIndex search value in the tree with a given index
func (record *Record) SearchValueIndex(name string, index []uint32) (adatypes.IAdaValue, error) {
	record.definition.Values = record.Value
	adatypes.Central.Log.Debugf("Record value : %#v", record.Value)
	return record.definition.SearchByIndex(name, index, false)
}

// PeriodGroup period group of field value
func PeriodGroup(v adatypes.IAdaValue) adatypes.IAdaValue {
	if v.Type().HasFlagSet(adatypes.FlagOptionPE) {
		c := v
		for c.Type().Type() != adatypes.FieldTypePeriodGroup {
			c = c.Parent()
		}
		return c
	}
	return nil
}

// ValueQuantity provide number of quantity in an PE or MU field
func (record *Record) ValueQuantity(param ...interface{}) int32 {
	if len(param) == 0 {
		return -1
	}

	var index []uint32
	fieldName := param[0].(string)
	adatypes.Central.Log.Debugf("Field name: %s", fieldName)

	if strings.ContainsRune(fieldName, '[') {
		index = extractIndex(fieldName)
		fieldName = fieldName[:strings.IndexRune(fieldName, '[')]
	} else {
		for i := 1; i < len(param); i++ {
			switch w := param[i].(type) {
			case uint32:
				index = append(index, w)
			case int:
				index = append(index, uint32(w))
			default:
			}
		}
	}
	adatypes.Central.Log.Debugf("Index from parser %#v", index)
	if v, ok := record.HashFields[fieldName]; ok {
		if v.Type().HasFlagSet(adatypes.FlagOptionPE) {
			adatypes.Central.Log.Debugf("Quantity of %s PE", v.Type().Name())
			if len(index) < 1 {
				p := PeriodGroup(v)
				pv := p.(*adatypes.StructureValue)
				return int32(pv.NrElements())
			}
			var err error
			adatypes.Central.Log.Debugf("Search index of PE %s", v.Type().Name())
			v, err = record.SearchValueIndex(fieldName, index)
			if err != nil {
				adatypes.Central.Log.Debugf("Error %s/%v: %v", fieldName, index, err)
				return -1
			}
			switch mv := v.(type) {
			case *adatypes.StructureValue:
				return int32(mv.NrElements())
			default:
			}
		}
		if v.Type().Type() == adatypes.FieldTypeMultiplefield {
			adatypes.Central.Log.Debugf("Quantity of %s MU elements", v.Type().Name())
			mv := v.(*adatypes.StructureValue)
			return int32(mv.NrElements())
		}
		return 1
	}
	return -1
}

// Scan scan for different field entries
func (record *Record) Scan(dest ...interface{}) (err error) {
	adatypes.Central.Log.Debugf("Scan Record %#v", record.fields)
	if f, ok := record.fields["#ISN"]; ok {
		adatypes.Central.Log.Debugf("Fill Record ISN=%d", record.Isn)
		*(dest[f.index].(*int)) = int(record.Isn)
	}
	if f, ok := record.fields["#ISNQUANTITY"]; ok {
		adatypes.Central.Log.Debugf("Fill Record ISN quantity=%d", record.Quantity)
		*(dest[f.index].(*int)) = int(record.Quantity)
	}
	// Traverse to current entries
	tm := adatypes.TraverserValuesMethods{EnterFunction: scanFieldsTraverser}
	sf := &scanFields{fields: record.fields, parameter: dest}
	_, err = record.Traverse(tm, sf)
	if err != nil {
		return err
	}
	return nil

}

func traverseMarshalXML2(adaValue adatypes.IAdaValue, x interface{}) (adatypes.TraverseResult, error) {
	enc := x.(*xml.Encoder)
	if adaValue.Type().IsStructure() {
		switch adaValue.Type().Type() {
		case adatypes.FieldTypePeriodGroup:
			peName := "Period"
			start := xml.StartElement{Name: xml.Name{Local: peName}}
			if adaValue.Type().Name() != adaValue.Type().ShortName() {
				peName = adaValue.Type().Name()
			} else {
				attrs := make([]xml.Attr, 0)
				attrs = append(attrs, xml.Attr{Name: xml.Name{Local: "sn"}, Value: adaValue.Type().Name()})
				start.Attr = attrs
			}
			enc.EncodeToken(start)
		case adatypes.FieldTypeMultiplefield:
			muName := "Multiple"
			if adaValue.Type().Name() != adaValue.Type().ShortName() {
				muName = adaValue.Type().Name()
				start := xml.StartElement{Name: xml.Name{Local: muName}}
				enc.EncodeToken(start)
			} else {
				start := xml.StartElement{Name: xml.Name{Local: muName}}
				attrs := make([]xml.Attr, 0)
				attrs = append(attrs, xml.Attr{Name: xml.Name{Local: "sn"}, Value: adaValue.Type().Name()})
				start.Attr = attrs
				enc.EncodeToken(start)
			}
		case adatypes.FieldTypeGroup:
			grName := "Group"
			if adaValue.Type().Name() != adaValue.Type().ShortName() {
				grName = adaValue.Type().Name()
				start := xml.StartElement{Name: xml.Name{Local: grName}}
				enc.EncodeToken(start)
			} else {
				start := xml.StartElement{Name: xml.Name{Local: grName}}
				attrs := make([]xml.Attr, 0)
				attrs = append(attrs, xml.Attr{Name: xml.Name{Local: "sn"}, Value: adaValue.Type().Name()})
				start.Attr = attrs
				enc.EncodeToken(start)
			}
		default:
			start := xml.StartElement{Name: xml.Name{Local: adaValue.Type().Name()}}
			enc.EncodeToken(start)
		}
	} else {
		isLink := strings.HasPrefix(adaValue.Type().Name(), "@")
		name := adaValue.Type().Name()
		if isLink {
			name = adaValue.Type().Name()[1:]
		}
		start := xml.StartElement{Name: xml.Name{Local: name}}
		if isLink {
			start.Attr = []xml.Attr{xml.Attr{Name: xml.Name{Local: "type"}, Value: "link"}}
		}
		enc.EncodeToken(start)
		x := adaValue.String()
		x = strings.Trim(x, " ")
		enc.EncodeToken(xml.CharData([]byte(x)))
		enc.EncodeToken(start.End())
	}
	return adatypes.Continue, nil
}

func traverseMarshalXMLEnd2(adaValue adatypes.IAdaValue, x interface{}) (adatypes.TraverseResult, error) {
	if adaValue.Type().IsStructure() {
		enc := x.(*xml.Encoder)
		sv := adaValue.(*adatypes.StructureValue)
		if adaValue.Type().Type() == adatypes.FieldTypePeriodGroup && sv.NrElements() > 0 {
			end := xml.EndElement{Name: xml.Name{Local: "Entry"}}
			enc.EncodeToken(end)
		}
		if adaValue.Type().Type() == adatypes.FieldTypePeriodGroup {
			end := xml.EndElement{Name: xml.Name{Local: "Period"}}
			enc.EncodeToken(end)
		}
		if adaValue.Type().Type() == adatypes.FieldTypeMultiplefield {
			end := xml.EndElement{Name: xml.Name{Local: "Multiple"}}
			enc.EncodeToken(end)
		}
		if adaValue.Type().Type() == adatypes.FieldTypeGroup {
			end := xml.EndElement{Name: xml.Name{Local: "Group"}}
			enc.EncodeToken(end)
		}
		end := xml.EndElement{Name: xml.Name{Local: adaValue.Type().Name()}}
		enc.EncodeToken(end)
	}
	return adatypes.Continue, nil
}

func traverseMarshalXMLElement(adaValue adatypes.IAdaValue, nr, max int, x interface{}) (adatypes.TraverseResult, error) {
	enc := x.(*xml.Encoder)
	if adaValue.Type().Type() == adatypes.FieldTypePeriodGroup {
		if nr > 0 {
			end := xml.EndElement{Name: xml.Name{Local: "Entry"}}
			enc.EncodeToken(end)
		}
		start := xml.StartElement{Name: xml.Name{Local: "Entry"}}
		enc.EncodeToken(start)
	}
	return adatypes.Continue, nil
}

// MarshalXML provide XML
func (record *Record) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	adatypes.Central.Log.Debugf("Marshal XML record: %d", record.Isn)
	var rec xml.StartElement
	adatypes.Central.Log.Debugf("Map usage: %#v", record.adabasMap)
	if record.adabasMap != nil {
		rec = xml.StartElement{Name: xml.Name{Local: record.adabasMap.Name}}
	} else {
		rec = xml.StartElement{Name: xml.Name{Local: "Record"}}
	}
	if record.Isn > 0 {
		rec.Attr = []xml.Attr{xml.Attr{Name: xml.Name{Local: "ISN"}, Value: strconv.Itoa(int(record.Isn))}}
	}
	if record.Quantity > 0 {
		rec.Attr = []xml.Attr{xml.Attr{Name: xml.Name{Local: "Quantity"}, Value: strconv.Itoa(int(record.Quantity))}}
	}
	e.EncodeToken(rec)
	tm := adatypes.TraverserValuesMethods{EnterFunction: traverseMarshalXML2, LeaveFunction: traverseMarshalXMLEnd2, ElementFunction: traverseMarshalXMLElement}
	record.Traverse(tm, e)
	e.EncodeToken(rec.End())
	adatypes.Central.Log.Debugf("Marshal XML record finished")

	return nil
}

// MarshalJSON provide JSON
func (record *Record) MarshalJSON() ([]byte, error) {
	adatypes.Central.Log.Debugf("Marshal JSON record: %d", record.Isn)
	req := &responseJSON{special: true}
	tm := adatypes.TraverserValuesMethods{EnterFunction: traverseMarshalJSON, LeaveFunction: traverseMarshalJSONEnd,
		ElementFunction: traverseElementMarshalJSON}
	req.stack = adatypes.NewStack()

	dataMap := make(map[string]interface{})
	req.dataMap = &dataMap
	req.Values = append(req.Values, req.dataMap)
	if record.Isn > 0 {
		dataMap["ISN"] = record.Isn
	}
	if record.Quantity > 0 {
		dataMap["Quantity"] = record.Quantity
	}

	// Traverse record generating JSON
	_, err := record.Traverse(tm, req)
	if err != nil {
		return nil, err
	}
	adatypes.Central.Log.Debugf("Go JSON response %v -> %s", err, req.buffer.String())

	return json.Marshal(req.dataMap)
}
