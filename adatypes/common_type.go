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

package adatypes

import (
	"encoding/binary"
)

// FieldType indicate a field type of the field
type FieldType uint

const (
	// FieldTypeUndefined field type undefined
	FieldTypeUndefined FieldType = iota
	// FieldTypeUByte field type unsigned byte
	FieldTypeUByte
	// FieldTypeByte field type signed byte
	FieldTypeByte
	// FieldTypeUInt2 field type unsigned integer of 2 bytes
	FieldTypeUInt2
	// FieldTypeInt2 field type signed integer of 2 bytes
	FieldTypeInt2
	// FieldTypeShort field type signed short
	FieldTypeShort
	// FieldTypeUInt4 field type unsigned integer of 4 bytes
	FieldTypeUInt4
	// FieldTypeUInt4Array field type array unsigned integer of 4 bytes
	FieldTypeUInt4Array
	// FieldTypeInt4 field type signed integer of 4 bytes
	FieldTypeInt4
	// FieldTypeUInt8 field type unsigned integer of 8 bytes
	FieldTypeUInt8
	// FieldTypeInt8 field type signed integer of 8 bytes
	FieldTypeInt8
	// FieldTypeLong field type signed long
	FieldTypeLong
	// FieldTypePacked field type packed
	FieldTypePacked
	// FieldTypeUnpacked field type unpacked
	FieldTypeUnpacked
	// FieldTypeDouble field type double
	FieldTypeDouble
	// FieldTypeFloat field type float
	FieldTypeFloat
	// FieldTypeFiller field type for fill gaps between struct types
	FieldTypeFiller
	// FieldTypeString field type string
	FieldTypeString
	// FieldTypeByteArray field type byte array
	FieldTypeByteArray
	// FieldTypeCharacter field type character
	FieldTypeCharacter
	// FieldTypeLength field type for length definitions
	FieldTypeLength
	// FieldTypeUnicode field type unicode string
	FieldTypeUnicode
	// FieldTypeLAUnicode field type unicode large objects
	FieldTypeLAUnicode
	// FieldTypeLBUnicode field type unicode LOB
	FieldTypeLBUnicode
	// FieldTypeLAString field type string large objects
	FieldTypeLAString
	// FieldTypeLBString field type string LOB
	FieldTypeLBString
	// FieldTypeFieldLength field length
	FieldTypeFieldLength
	// FieldTypePeriodGroup field type period group
	FieldTypePeriodGroup
	// FieldTypeMultiplefield field type multiple fields
	FieldTypeMultiplefield
	// FieldTypeStructure field type of structured types
	FieldTypeStructure
	// FieldTypeGroup field type group
	FieldTypeGroup
	// FieldTypePackedArray field type packed array
	FieldTypePackedArray
	// FieldTypePhonetic field type of phonetic descriptor
	FieldTypePhonetic
	// FieldTypeSuperDesc field type of super descriptors
	FieldTypeSuperDesc
	// FieldTypeLiteral field type of literal data send to database
	FieldTypeLiteral
	// FieldTypeFieldCount field type to defined field count of MU or PE fields
	FieldTypeFieldCount
	// FieldTypeHyperDesc field type of Hyper descriptors
	FieldTypeHyperDesc
	// FieldTypeReferential field type for referential integrity
	FieldTypeReferential
	// FieldTypeCollation field type of collation descriptors
	FieldTypeCollation
	// FieldTypeFunction field type to define functions working on result list
	FieldTypeFunction
)

var typeName = []string{"Undefined", "UByte", "Byte", "UInt2", "Int2", "Short", "UInt4", "UInt4Array", "Int4", "UInt8", "Int8",
	"Long", "Packed", "Unpacked", "Double", "Float", "Filler", "String", "ByteArray", "Character", "Length",
	"Unicode", "LAUnicode", "LBUnicode", "LAString", "LBString", "FieldLength", "PeriodGroup", "Multiplefield",
	"Structure", "Group", "PackedArray", "Phonetic", "SuperDesc", "Literal", "FieldCount", "HyperDesc",
	"Referential", "Collation", "Function"}

func (fieldType FieldType) name() string {
	return typeName[fieldType]
}

// FormatCharacter format character use to output FDT
func (fieldType FieldType) FormatCharacter() string {
	switch fieldType {
	case FieldTypeCharacter, FieldTypeString, FieldTypeLAString, FieldTypeLBString:
		return "A"
	case FieldTypeUnicode, FieldTypeLAUnicode, FieldTypeLBUnicode:
		return "W"
	case FieldTypeUByte, FieldTypeUInt2, FieldTypeUInt4, FieldTypeUInt8, FieldTypeShort, FieldTypeByteArray:
		return "B"
	case FieldTypePacked:
		return "P"
	case FieldTypeUnpacked:
		return "U"
	case FieldTypeByte, FieldTypeInt2, FieldTypeInt4, FieldTypeInt8:
		return "F"
	case FieldTypeFloat:
		return "G"
	default:
	}
	return " "
}

// CommonType common data type structure defined for all types
type CommonType struct {
	fieldType           FieldType
	name                string
	shortName           string
	length              uint32
	level               uint8
	flags               uint8
	parentType          IAdaType
	options             uint32
	Charset             string
	endian              binary.ByteOrder
	peRange             AdaRange
	muRange             AdaRange
	FormatTypeCharacter rune
	FormatLength        uint32
}

// Type returns field type of the field
func (commonType *CommonType) Type() FieldType {
	return commonType.fieldType
}

// Name return the name of the field
func (commonType *CommonType) Name() string {
	return commonType.name
}

// ShortName return the short name of the field
func (commonType *CommonType) ShortName() string {
	return commonType.shortName
}

// SetName set the name of the field
func (commonType *CommonType) SetName(name string) {
	commonType.name = name
}

// Level Type return level of the field
func (commonType *CommonType) Level() uint8 {
	return commonType.level
}

// SetLevel Set Adabas level of the field
func (commonType *CommonType) SetLevel(level uint8) {
	commonType.level = level
}

// Endian Get data endian
func (commonType *CommonType) Endian() binary.ByteOrder {
	if commonType.endian == nil {
		commonType.endian = endian()
	}
	return commonType.endian
}

// SetEndian Set data endian
func (commonType *CommonType) SetEndian(endian binary.ByteOrder) {
	commonType.endian = endian
}

// SetRange set Adabas range
func (commonType *CommonType) SetRange(r *AdaRange) {
	commonType.peRange = *r
}

// SetParent set the parent of the type
func (commonType *CommonType) SetParent(parentType IAdaType) {
	if parentType != nil {
		Central.Log.Debugf("%s parent is set to %s", commonType.name, parentType.Name())
		if parentType.HasFlagSet(FlagOptionPE) {
			Central.Log.Debugf("Inherit PE flag from parent %s to %s %p->%p", parentType.Name(), commonType.Name(), commonType, parentType)
			commonType.AddFlag(FlagOptionPE)
		}
		if commonType.HasFlagSet(FlagOptionMU) {
			p := parentType
			for p != nil {
				if p.GetParent() != nil {
					Central.Log.Debugf("Inherit MU flag to parent from %s to %s (%p)", commonType.Name(), p.Name(), p)
					p.AddFlag(FlagOptionMU)
					Central.Log.Debugf("Flag set? %v", p.HasFlagSet(FlagOptionMU))
				}
				p = p.GetParent()
			}
		}
	} else {
		Central.Log.Debugf("%s: Reset parent type to nil of %p", commonType.name, commonType)
		if commonType.parentType != nil {
			pType := commonType.parentType.(*StructureType)
			pType.RemoveField(commonType)
		}
	}
	commonType.parentType = parentType
}

// GetParent get the parent defined to this type
func (commonType *CommonType) GetParent() IAdaType {
	return commonType.parentType
}

// IsStructure return if the type is of structure types
func (commonType *CommonType) IsStructure() bool {
	return false
}

// AddOption add the option to the field
func (commonType *CommonType) AddOption(fieldOption FieldOption) {
	commonType.options |= (1 << fieldOption)
}

// ClearOption clear the option to the field
func (commonType *CommonType) ClearOption(fieldOption FieldOption) {
	commonType.options &^= (1 << fieldOption)
}

// IsOption Check if the option of the field is set
func (commonType *CommonType) IsOption(fieldOption FieldOption) bool {
	return (commonType.options & (1 << fieldOption)) != 0
}

// SetOption Set all options of the field
func (commonType *CommonType) SetOption(option uint32) {
	commonType.options = option
}

// IsSpecialDescriptor return true if it is a special descriptor
func (commonType *CommonType) IsSpecialDescriptor() bool {
	switch commonType.fieldType {
	case FieldTypeCollation, FieldTypePhonetic, FieldTypeSuperDesc,
		FieldTypeHyperDesc, FieldTypeReferential:
		return true
	default:

	}
	return false
}

// FieldOption type for field option
type FieldOption uint32

const (
	// FieldOptionUQ field option for unique descriptors
	FieldOptionUQ FieldOption = iota
	// FieldOptionNU field option for null suppression
	FieldOptionNU
	// FieldOptionFI field option for fixed size
	FieldOptionFI
	// FieldOptionDE field option for descriptors
	FieldOptionDE
	// FieldOptionNC field option for sql
	FieldOptionNC
	// FieldOptionNN field option for non null
	FieldOptionNN
	// FieldOptionHF field option for high order fields
	FieldOptionHF
	// FieldOptionNV field option for null value
	FieldOptionNV
	// FieldOptionNB field option for
	FieldOptionNB
	// FieldOptionHE field option for
	FieldOptionHE
	// FieldOptionPE field option for period
	FieldOptionPE
	// FieldOptionMU field option for multiple fields
	FieldOptionMU
	// FieldOptionLA field option for large alpha
	FieldOptionLA
	// FieldOptionLB field option for large objects
	FieldOptionLB
	// FieldOptionColExit field option for collation exit
	FieldOptionColExit
)

var fieldOptions = []string{"UQ", "NU", "FI", "DE", "NC", "NN", "HF", "NV", "NB", "HE", "PE", "MU"}

// FlagOption flag option used to omit traversal through the tree (example is MU and PE)
type FlagOption uint

const (
	// FlagOptionPE indicate tree is part of period group
	FlagOptionPE FlagOption = iota
	// FlagOptionMU indicate tree contains MU fields
	FlagOptionMU
	// FlagOptionMUGhost ghost field for MU
	FlagOptionMUGhost
	// FlagOptionToBeRemoved should be removed
	FlagOptionToBeRemoved
	// FlagOptionSecondCall Field will need a second call to get the value
	FlagOptionSecondCall
)

// Bit return the Bit of the option flag
func (flagOption FlagOption) Bit() uint8 {
	return (1 << flagOption)
}

// HasFlagSet check if given flag is set
func (commonType *CommonType) HasFlagSet(flagOption FlagOption) bool {
	return (commonType.flags & flagOption.Bit()) != 0
}

// AddFlag add the flag to the type flag set
func (commonType *CommonType) AddFlag(flagOption FlagOption) {
	commonType.flags |= flagOption.Bit()
	if flagOption == FlagOptionMU {
		p := commonType.GetParent()
		for p != nil {
			p.AddFlag(flagOption)
			p = p.GetParent()
		}
	}
}

// RemoveFlag add the flag to the type flag set
func (commonType *CommonType) RemoveFlag(flagOption FlagOption) {
	commonType.flags &= ^flagOption.Bit()
}
