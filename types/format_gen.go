package types

// nolint

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *CommonFormatEvent) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zbai uint32
	zbai, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zbai > 0 {
		zbai--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Type":
			z.Type, err = dc.ReadString()
			if err != nil {
				return
			}
		case "Key":
			var zcmr uint32
			zcmr, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Key) >= int(zcmr) {
				z.Key = (z.Key)[:zcmr]
			} else {
				z.Key = make([]interface{}, zcmr)
			}
			for zxvk := range z.Key {
				z.Key[zxvk], err = dc.ReadIntf()
				if err != nil {
					return
				}
			}
		case "SeqNo":
			z.SeqNo, err = dc.ReadUint64()
			if err != nil {
				return
			}
		case "Timestamp":
			z.Timestamp, err = dc.ReadInt64()
			if err != nil {
				return
			}
		case "Fields":
			if dc.IsNil() {
				err = dc.ReadNil()
				if err != nil {
					return
				}
				z.Fields = nil
			} else {
				if z.Fields == nil {
					z.Fields = new([]CommonFormatField)
				}
				var zajw uint32
				zajw, err = dc.ReadArrayHeader()
				if err != nil {
					return
				}
				if cap(*z.Fields) >= int(zajw) {
					*z.Fields = (*z.Fields)[:zajw]
				} else {
					*z.Fields = make([]CommonFormatField, zajw)
				}
				for zbzg := range *z.Fields {
					var zwht uint32
					zwht, err = dc.ReadMapHeader()
					if err != nil {
						return
					}
					for zwht > 0 {
						zwht--
						field, err = dc.ReadMapKeyPtr()
						if err != nil {
							return
						}
						switch msgp.UnsafeString(field) {
						case "Name":
							(*z.Fields)[zbzg].Name, err = dc.ReadString()
							if err != nil {
								return
							}
						case "Value":
							(*z.Fields)[zbzg].Value, err = dc.ReadIntf()
							if err != nil {
								return
							}
						default:
							err = dc.Skip()
							if err != nil {
								return
							}
						}
					}
				}
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *CommonFormatEvent) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 5
	// write "Type"
	err = en.Append(0x85, 0xa4, 0x54, 0x79, 0x70, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Type)
	if err != nil {
		return
	}
	// write "Key"
	err = en.Append(0xa3, 0x4b, 0x65, 0x79)
	if err != nil {
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.Key)))
	if err != nil {
		return
	}
	for zxvk := range z.Key {
		err = en.WriteIntf(z.Key[zxvk])
		if err != nil {
			return
		}
	}
	// write "SeqNo"
	err = en.Append(0xa5, 0x53, 0x65, 0x71, 0x4e, 0x6f)
	if err != nil {
		return err
	}
	err = en.WriteUint64(z.SeqNo)
	if err != nil {
		return
	}
	// write "Timestamp"
	err = en.Append(0xa9, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70)
	if err != nil {
		return err
	}
	err = en.WriteInt64(z.Timestamp)
	if err != nil {
		return
	}
	// write "Fields"
	err = en.Append(0xa6, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x73)
	if err != nil {
		return err
	}
	if z.Fields == nil {
		err = en.WriteNil()
		if err != nil {
			return
		}
	} else {
		err = en.WriteArrayHeader(uint32(len(*z.Fields)))
		if err != nil {
			return
		}
		for zbzg := range *z.Fields {
			// map header, size 2
			// write "Name"
			err = en.Append(0x82, 0xa4, 0x4e, 0x61, 0x6d, 0x65)
			if err != nil {
				return err
			}
			err = en.WriteString((*z.Fields)[zbzg].Name)
			if err != nil {
				return
			}
			// write "Value"
			err = en.Append(0xa5, 0x56, 0x61, 0x6c, 0x75, 0x65)
			if err != nil {
				return err
			}
			err = en.WriteIntf((*z.Fields)[zbzg].Value)
			if err != nil {
				return
			}
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *CommonFormatEvent) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 5
	// string "Type"
	o = append(o, 0x85, 0xa4, 0x54, 0x79, 0x70, 0x65)
	o = msgp.AppendString(o, z.Type)
	// string "Key"
	o = append(o, 0xa3, 0x4b, 0x65, 0x79)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Key)))
	for zxvk := range z.Key {
		o, err = msgp.AppendIntf(o, z.Key[zxvk])
		if err != nil {
			return
		}
	}
	// string "SeqNo"
	o = append(o, 0xa5, 0x53, 0x65, 0x71, 0x4e, 0x6f)
	o = msgp.AppendUint64(o, z.SeqNo)
	// string "Timestamp"
	o = append(o, 0xa9, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70)
	o = msgp.AppendInt64(o, z.Timestamp)
	// string "Fields"
	o = append(o, 0xa6, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x73)
	if z.Fields == nil {
		o = msgp.AppendNil(o)
	} else {
		o = msgp.AppendArrayHeader(o, uint32(len(*z.Fields)))
		for zbzg := range *z.Fields {
			// map header, size 2
			// string "Name"
			o = append(o, 0x82, 0xa4, 0x4e, 0x61, 0x6d, 0x65)
			o = msgp.AppendString(o, (*z.Fields)[zbzg].Name)
			// string "Value"
			o = append(o, 0xa5, 0x56, 0x61, 0x6c, 0x75, 0x65)
			o, err = msgp.AppendIntf(o, (*z.Fields)[zbzg].Value)
			if err != nil {
				return
			}
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *CommonFormatEvent) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zhct uint32
	zhct, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for zhct > 0 {
		zhct--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Type":
			z.Type, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "Key":
			var zcua uint32
			zcua, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Key) >= int(zcua) {
				z.Key = (z.Key)[:zcua]
			} else {
				z.Key = make([]interface{}, zcua)
			}
			for zxvk := range z.Key {
				z.Key[zxvk], bts, err = msgp.ReadIntfBytes(bts)
				if err != nil {
					return
				}
			}
		case "SeqNo":
			z.SeqNo, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				return
			}
		case "Timestamp":
			z.Timestamp, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				return
			}
		case "Fields":
			if msgp.IsNil(bts) {
				bts, err = msgp.ReadNilBytes(bts)
				if err != nil {
					return
				}
				z.Fields = nil
			} else {
				if z.Fields == nil {
					z.Fields = new([]CommonFormatField)
				}
				var zxhx uint32
				zxhx, bts, err = msgp.ReadArrayHeaderBytes(bts)
				if err != nil {
					return
				}
				if cap(*z.Fields) >= int(zxhx) {
					*z.Fields = (*z.Fields)[:zxhx]
				} else {
					*z.Fields = make([]CommonFormatField, zxhx)
				}
				for zbzg := range *z.Fields {
					var zlqf uint32
					zlqf, bts, err = msgp.ReadMapHeaderBytes(bts)
					if err != nil {
						return
					}
					for zlqf > 0 {
						zlqf--
						field, bts, err = msgp.ReadMapKeyZC(bts)
						if err != nil {
							return
						}
						switch msgp.UnsafeString(field) {
						case "Name":
							(*z.Fields)[zbzg].Name, bts, err = msgp.ReadStringBytes(bts)
							if err != nil {
								return
							}
						case "Value":
							(*z.Fields)[zbzg].Value, bts, err = msgp.ReadIntfBytes(bts)
							if err != nil {
								return
							}
						default:
							bts, err = msgp.Skip(bts)
							if err != nil {
								return
							}
						}
					}
				}
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *CommonFormatEvent) Msgsize() (s int) {
	s = 1 + 5 + msgp.StringPrefixSize + len(z.Type) + 4 + msgp.ArrayHeaderSize
	for zxvk := range z.Key {
		s += msgp.GuessSize(z.Key[zxvk])
	}
	s += 6 + msgp.Uint64Size + 10 + msgp.Int64Size + 7
	if z.Fields == nil {
		s += msgp.NilSize
	} else {
		s += msgp.ArrayHeaderSize
		for zbzg := range *z.Fields {
			s += 1 + 5 + msgp.StringPrefixSize + len((*z.Fields)[zbzg].Name) + 6 + msgp.GuessSize((*z.Fields)[zbzg].Value)
		}
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *CommonFormatField) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zdaf uint32
	zdaf, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zdaf > 0 {
		zdaf--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Name":
			z.Name, err = dc.ReadString()
			if err != nil {
				return
			}
		case "Value":
			z.Value, err = dc.ReadIntf()
			if err != nil {
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z CommonFormatField) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "Name"
	err = en.Append(0x82, 0xa4, 0x4e, 0x61, 0x6d, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Name)
	if err != nil {
		return
	}
	// write "Value"
	err = en.Append(0xa5, 0x56, 0x61, 0x6c, 0x75, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteIntf(z.Value)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z CommonFormatField) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "Name"
	o = append(o, 0x82, 0xa4, 0x4e, 0x61, 0x6d, 0x65)
	o = msgp.AppendString(o, z.Name)
	// string "Value"
	o = append(o, 0xa5, 0x56, 0x61, 0x6c, 0x75, 0x65)
	o, err = msgp.AppendIntf(o, z.Value)
	if err != nil {
		return
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *CommonFormatField) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zpks uint32
	zpks, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for zpks > 0 {
		zpks--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Name":
			z.Name, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "Value":
			z.Value, bts, err = msgp.ReadIntfBytes(bts)
			if err != nil {
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z CommonFormatField) Msgsize() (s int) {
	s = 1 + 5 + msgp.StringPrefixSize + len(z.Name) + 6 + msgp.GuessSize(z.Value)
	return
}
