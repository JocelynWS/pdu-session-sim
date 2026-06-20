package models

import (
	"encoding/binary"
	"errors"
	"net"
)

type PFCPHeader struct {
	VersionAndFlags uint8  // Byte 0: 0x20
	MessageType     uint8  // Byte 1: 50, 51, 52, 53
	Length          uint16 // Byte 2-3: Length of payload (after header, or total. We'll use payload length: 5 for req, 1 for res)
	SequenceNumber  uint32 // Byte 4-7: Sequence number
}

type PFCPSessionRequest struct {
	Header  PFCPHeader
	PdnType uint8  // Byte 8: 1=IPv4
	IP      net.IP // Byte 9-12: IP Address
}

type PFCPSessionResponse struct {
	Header PFCPHeader
	Cause  uint8 // Byte 8: Cause (1=Request accepted)
}

// EncodeRequest encodes a PFCPSessionRequest into a byte slice.
func EncodeRequest(msgType uint8, seq uint32, ip string) ([]byte, error) {
	parsedIP := net.ParseIP(ip).To4()
	if parsedIP == nil {
		return nil, errors.New("invalid IPv4 address")
	}

	buf := make([]byte, 13)
	buf[0] = 0x20
	buf[1] = msgType
	binary.BigEndian.PutUint16(buf[2:4], 5) // payload length is 5 bytes (1 byte PDN Type + 4 bytes IP)
	binary.BigEndian.PutUint32(buf[4:8], seq)
	buf[8] = 1 // PDN Type: 1 (IPv4)
	copy(buf[9:13], parsedIP)

	return buf, nil
}

// DecodeRequest decodes a byte slice into a PFCPSessionRequest.
func DecodeRequest(data []byte) (*PFCPSessionRequest, error) {
	if len(data) < 13 {
		return nil, errors.New("data too short for request")
	}
	if data[0] != 0x20 {
		return nil, errors.New("invalid PFCP version/flags")
	}

	req := &PFCPSessionRequest{}
	req.Header.VersionAndFlags = data[0]
	req.Header.MessageType = data[1]
	req.Header.Length = binary.BigEndian.Uint16(data[2:4])
	req.Header.SequenceNumber = binary.BigEndian.Uint32(data[4:8])
	req.PdnType = data[8]
	req.IP = net.IP(data[9:13])

	return req, nil
}

// EncodeResponse encodes a PFCPSessionResponse into a byte slice.
func EncodeResponse(msgType uint8, seq uint32, cause uint8) ([]byte, error) {
	buf := make([]byte, 9)
	buf[0] = 0x20
	buf[1] = msgType
	binary.BigEndian.PutUint16(buf[2:4], 1) // payload length is 1 byte (Cause)
	binary.BigEndian.PutUint32(buf[4:8], seq)
	buf[8] = cause

	return buf, nil
}

// DecodeResponse decodes a byte slice into a PFCPSessionResponse.
func DecodeResponse(data []byte) (*PFCPSessionResponse, error) {
	if len(data) < 9 {
		return nil, errors.New("data too short for response")
	}
	if data[0] != 0x20 {
		return nil, errors.New("invalid PFCP version/flags")
	}

	res := &PFCPSessionResponse{}
	res.Header.VersionAndFlags = data[0]
	res.Header.MessageType = data[1]
	res.Header.Length = binary.BigEndian.Uint16(data[2:4])
	res.Header.SequenceNumber = binary.BigEndian.Uint32(data[4:8])
	res.Cause = data[8]

	return res, nil
}
