package adatypes

import (
	"fmt"
	"strings"
)

// StructureTraverser structure traverser interface
type StructureTraverser interface {
	Traverse(t TraverserMethods, level int, x interface{}) (err error)
}

// StructureType creates a new structure type
type StructureType struct {
	CommonType
	occ       int
	condition FieldCondition
	SubTypes  []IAdaType
	fieldMap  map[string]IAdaType
}

// NewStructure Creates a new object of structured list types
func NewStructure() *StructureType {
	Central.Log.Debugf("Create structure list")
	return &StructureType{
		CommonType: CommonType{
			flags: uint32(1 << FlagOptionToBeRemoved),
		},
		condition: FieldCondition{
			lengthFieldIndex: -1,
			refField:         NoReferenceField,
		},
	}
}

// NewStructureEmpty Creates a new object of structured list types
func NewStructureEmpty(fType FieldType, name string, occByteShort int16,
	level uint8) *StructureType {
	Central.Log.Debugf("Create empty structure list %s with type %d ", name, fType)
	var pr *AdaRange
	var mr *AdaRange
	switch fType {
	case FieldTypePeriodGroup:
		pr = NewRange(1, lastEntry)
		mr = NewEmptyRange()
	case FieldTypeMultiplefield:
		pr = NewEmptyRange()
		mr = NewRange(1, lastEntry)
	default:
		pr = NewEmptyRange()
		mr = NewEmptyRange()
	}
	st := &StructureType{
		CommonType: CommonType{
			fieldType: fType,
			name:      name,
			flags:     uint32(1 << FlagOptionToBeRemoved),
			shortName: name,
			length:    0,
			peRange:   *pr,
			muRange:   *mr,
			level:     level,
		},
		occ: int(occByteShort),
		condition: FieldCondition{
			lengthFieldIndex: -1,
			refField:         NoReferenceField,
		},
	}
	st.adaptSubFields()
	Central.Log.Debugf("Got structure list Range [%s,%s]", st.peRange.FormatBuffer(), st.muRange.FormatBuffer())
	return st
}

// NewStructureList Creates a new object of structured list types
func NewStructureList(fType FieldType, name string, occByteShort int16, subFields []IAdaType) *StructureType {
	Central.Log.Debugf("Create new structure list %s types=%d type=%s", name, len(subFields), fType.name())
	st := &StructureType{
		CommonType: CommonType{fieldType: fType,
			name:      name,
			shortName: name,
			flags:     uint32(1 << FlagOptionToBeRemoved),
			level:     1,
			length:    0},
		occ: int(occByteShort),
		condition: FieldCondition{
			lengthFieldIndex: -1,
			refField:         NoReferenceField,
		},
		SubTypes: subFields,
	}
	switch fType {
	case FieldTypePeriodGroup:
		st.peRange = *NewRange(1, lastEntry)
	case FieldTypeMultiplefield:
		st.muRange = *NewRange(1, lastEntry)
	default:
	}
	st.adaptSubFields()
	// Central.Log.Debugf("Got structure list Range %s %s", st.peRange.FormatBuffer(), st.muRange.FormatBuffer())

	return st
}

// NewLongNameStructureList Creates a new object of structured list types
func NewLongNameStructureList(fType FieldType, name string, shortName string, occByteShort int16, subFields []IAdaType) *StructureType {
	st := NewStructureList(fType, name, occByteShort, subFields)
	st.shortName = shortName
	return st
}

// NewStructureCondition Creates a new object of structured list types
func NewStructureCondition(fType FieldType, name string, subFields []IAdaType, condition FieldCondition) *StructureType {
	Central.Log.Debugf("Create new structure with condition %s types=%d type=%d", name, len(subFields), fType)
	for _, t := range subFields {
		t.SetLevel(2)
	}
	return &StructureType{
		CommonType: CommonType{fieldType: fType,
			name:      name,
			shortName: name,
			flags:     uint32(1 << FlagOptionToBeRemoved),
			level:     1,
			length:    0},
		condition: condition,
		SubTypes:  subFields,
	}
}

func (adaType *StructureType) adaptSubFields() {
	if adaType.Type() == FieldTypePeriodGroup {
		Central.Log.Debugf("%s: set PE flag", adaType.Name())
		adaType.AddFlag(FlagOptionPE)
		adaType.occ = OccCapacity
	}
	if adaType.Type() == FieldTypeMultiplefield {
		Central.Log.Debugf("%s: set MU flag", adaType.Name())
		adaType.AddFlag(FlagOptionMU)
		adaType.occ = OccCapacity
	}
	for _, s := range adaType.SubTypes {
		s.SetParent(adaType)
		s.SetRange(&adaType.peRange)
		if adaType.Type() == FieldTypePeriodGroup {
			s.AddFlag(FlagOptionPE)
		}
		if adaType.HasFlagSet(FlagOptionPE) {
			s.AddFlag(FlagOptionPE)
		}

		if adaType.Type() == FieldTypeMultiplefield {
			Central.Log.Debugf("%s: set MU flag", adaType.Name())
			adaType.AddFlag(FlagOptionMU)
			s.AddFlag(FlagOptionMUGhost)
			if adaType.HasFlagSet(FlagOptionPE) {
				s.AddFlag(FlagOptionSecondCall)
			}
		}

	}
}

// String return the name of the field
func (adaType *StructureType) String() string {

	y := strings.Repeat(" ", int(adaType.level))
	Central.Log.Debugf("FS: %s -> %d", adaType.Name(), len(adaType.SubTypes))
	if adaType.fieldType == FieldTypeMultiplefield {
		if len(adaType.SubTypes) == 0 {
			return fmt.Sprintf("%s%d %s deleted", y, adaType.level, adaType.shortName)
		}
		options := adaType.SubTypes[0].Option()
		if options == "" {
			options = ",MU"
		} else {
			options = "," + strings.Replace(options, " ", ",", -1)
		}

		return fmt.Sprintf("%s%d, %s, %d, %s %s; %s", y, adaType.level, adaType.shortName, adaType.SubTypes[0].Length(),
			adaType.SubTypes[0].Type().FormatCharacter(), options, adaType.name)

	}
	options := adaType.Option()
	if options != "" {
		options = "," + strings.Replace(options, " ", ",", -1)
	}
	return fmt.Sprintf("%s%d, %s %s ; %s", y, adaType.level, adaType.shortName, options,
		adaType.name)
}

// Length returns the length of the field
func (adaType *StructureType) Length() uint32 {
	return adaType.length
}

// SetLength set the length of the field
func (adaType *StructureType) SetLength(length uint32) {
	adaType.length = length
}

// IsStructure return the structure of the field
func (adaType *StructureType) IsStructure() bool {
	return true
}

// NrFields number of fields contained in the structure
func (adaType *StructureType) NrFields() int {
	return len(adaType.SubTypes)
}

// func (adaType *StructureType) parseBuffer(helper *BufferHelper, option *BufferOption) {
// 	Central.Log.Debugf("Parse Structure type offset=%d", helper.offset)
// }

// Traverse Traverse through the definition tree calling a callback method for each node
func (adaType *StructureType) Traverse(t TraverserMethods, level int, x interface{}) (err error) {
	Central.Log.Debugf("Current structure -> %s", adaType.name)
	Central.Log.Debugf("Nr of structure types -> %v", len(adaType.SubTypes))
	for _, v := range adaType.SubTypes {
		Central.Log.Debugf("Traverse on %s/%s", v.Name(), v.ShortName())
		err = t.EnterFunction(v, adaType, level, x)
		if err != nil {
			return
		}
		if v.IsStructure() {
			Central.Log.Debugf("Traverse into structure %s", v.Name())
			err = v.(StructureTraverser).Traverse(t, level+1, x)
			if err != nil {
				return
			}
			if t.leaveFunction != nil {
				err = t.leaveFunction(v, adaType, level, x)
				if err != nil {
					return
				}
			}
		}
	}
	return nil
}

// AddField add a new field type into the structure type
func (adaType *StructureType) AddField(fieldType IAdaType) {
	Central.Log.Debugf("Add sub field %s on parent %s", fieldType.Name(), adaType.Name())
	fieldType.SetLevel(adaType.level + 1)
	fieldType.SetParent(adaType)
	fieldType.SetRange(&adaType.peRange)
	Central.Log.Debugf("Parent of %s is %s ", fieldType.Name(), fieldType.GetParent())
	if adaType.HasFlagSet(FlagOptionPE) {
		Central.Log.Debugf("Add sub field PE of parent %s field %s", adaType.Name(), fieldType.Name())
		fieldType.AddFlag(FlagOptionPE)
	}
	adaType.SubTypes = append(adaType.SubTypes, fieldType)
	if adaType.fieldMap == nil {
		adaType.fieldMap = make(map[string]IAdaType)
	}
	adaType.fieldMap[fieldType.Name()] = fieldType
	if fieldType.IsStructure() {
		st := fieldType.(*StructureType)
		st.fieldMap = adaType.fieldMap
	}
}

func travereAdaptPartOption(adaType IAdaType, parentType IAdaType, level int, x interface{}) error {
	adaType.AddFlag(FlagOptionPart)
	return nil
}

func (adaType *StructureType) addPart() {
	adaType.AddFlag(FlagOptionPart)
	t := TraverserMethods{EnterFunction: travereAdaptPartOption}
	adaType.Traverse(t, 0, nil)
}

// RemoveField remote field of the structure type
func (adaType *StructureType) RemoveField(fieldType *CommonType) {
	Central.Log.Debugf("Remove field %s out of %s nrFields=%d", fieldType.Name(), adaType.Name(), adaType.NrFields())
	// if adaType.NrFields() < 2 && adaType.GetParent() != nil {
	// 	Central.Log.Debugf("Only one left, remove last ", fieldType.Name())
	// 	commonType := &adaType.CommonType
	// 	adaType.GetParent().(*StructureType).RemoveField(commonType)
	// } else {
	Central.Log.Debugf("Rearrange, left=%d", adaType.NrFields())
	var newTypes []IAdaType
	for _, t := range adaType.SubTypes {
		if t.Name() != fieldType.Name() {
			newTypes = append(newTypes, t)
		}
	}
	adaType.SubTypes = newTypes
	// }
}

// SetRange set Adabas range
func (adaType *StructureType) SetRange(r *AdaRange) {
	adaType.peRange = *r
}

// Option return structure option as a string
func (adaType *StructureType) Option() string {
	switch adaType.fieldType {
	case FieldTypeMultiplefield:
		return "MU"
	case FieldTypePeriodGroup:
		return "PE"
	default:
	}
	return ""
}

// SetFractional set fractional part
func (adaType *StructureType) SetFractional(x uint32) {
}

// Fractional get fractional part
func (adaType *StructureType) Fractional() uint32 {
	return 0
}

// SetCharset set fractional part
func (adaType *StructureType) SetCharset(x string) {
}

// SetFormatType set format type
func (adaType *StructureType) SetFormatType(x rune) {
}

// FormatType get format type
func (adaType *StructureType) FormatType() rune {
	return adaType.FormatTypeCharacter
}

// SetFormatLength set format length
func (adaType *StructureType) SetFormatLength(x uint32) {
}

// Value return type specific value structure object
func (adaType *StructureType) Value() (adaValue IAdaValue, err error) {
	Central.Log.Debugf("Create structure type of %v", adaType.fieldType.name())
	switch adaType.fieldType {
	case FieldTypeStructure, FieldTypeGroup, FieldTypePeriodGroup, FieldTypeMultiplefield:
		Central.Log.Debugf("Return Structure value")
		adaValue = newStructure(adaType)
		return
	case FieldTypeRedefinition:
		adaValue = newRedefinition(adaType)
		return
	}
	Central.Log.Debugf("Return nil structure", adaType.String())
	err = NewGenericError(104, adaType.String())
	return
}

// ReplaceType replace type in the substructure
func (adaType *StructureType) ReplaceType(orgType, newType IAdaType) error {
	for i := 0; i < len(adaType.SubTypes); i++ {
		if adaType.SubTypes[i] == orgType {
			adaType.SubTypes[i] = newType
			return nil
		}
	}
	return NewGenericError(93, orgType.Name(), adaType.Name())
}
