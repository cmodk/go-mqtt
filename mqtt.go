package mqtt

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"

	"github.com/sirupsen/logrus"
)

var (
	log = logrus.New()
)

type mqttCommandPacketHandler func(*Server, *Client, Message) error
type SubscriptionHandler func(*Server, Message) error

type ConnectPacket struct {
	ProtocolName  string
	ProtocolLevel uint8
	ConnectFlags  uint8
	KeepAlive     uint16

	//For V5
	PropertiesLength uint8
	Properties       []uint8

	//Payload
	ClientId string
}

type Will struct {
	NumProperties VariableLengthInt
	Topic         string
	Payload       []byte
	QOS           int
	Retain        bool
}

type Client struct {
	Conn          net.Conn
	ProtocolLevel uint8
	Will          *Will
}

type Server struct {
	run      bool
	tls      *tls.Config
	handlers map[int]mqttCommandPacketHandler

	ServerSubscriptions map[*regexp.Regexp]SubscriptionHandler

	ClientSubscriptions map[*regexp.Regexp][]*Client
}

func (s *Server) Subscribe(topic string, qos int, handler SubscriptionHandler) error {

	exp := strings.Replace(topic, "+", "([^/\\n]*)", -1)
	log.Printf("Regexp: %s\n", exp)
	re := regexp.MustCompile(exp)

	s.ServerSubscriptions[re] = handler

	return nil
}

func (msg *Message) PayloadIndex() int {
	return len(msg.Payload)
}

func (msg *Message) AddVLI(vli VariableLengthInt) {
	data, _ := msg.PacketLength.ToBytes()
	msg.Payload = append(msg.Payload, data...)
	msg.PacketLength.Value += uint64(len(data))
	log.Printf("AddVLI: %v\n", msg.Payload)
}

func (msg *Message) AddBytes(data []byte) {
	msg.Payload = append(msg.Payload, data...)
	msg.PacketLength.Value += uint64(len(data))
}

func (msg *Message) AddString(data string) {
	msg.AddBytes([]byte(data))
}

func (msg *Message) AddUint16(value uint16) {
	hi := uint8(value >> 8)
	lo := uint8(value & 0xFF)

	msg.Payload = append(msg.Payload, hi, lo)
	msg.PacketLength.Value += uint64(2)
}

func (msg *Message) AddUint8(value uint8) {

	msg.Payload = append(msg.Payload, value)
	msg.PacketLength.Value += uint64(1)
}

func (s *Server) Publish(topic string, qos int, retain bool, payload []byte) error {

	msg := Message{
		PacketType:   3,
		PacketFlags:  0,
		PacketLength: VariableLengthInt{0},
		PacketId:     47,
		Sent:         false,
	}

	msg.AddUint16(uint16(len(topic)))
	msg.AddString(topic)
	//msg.AddUint16(msg.PacketId)
	msg.AddUint8(0) //No properties
	msg.AddBytes(payload)

	log.Printf("ClientSubscriptions: %v\n", s.ClientSubscriptions)
	for re, clients := range s.ClientSubscriptions {
		if re.MatchString(topic) {
			log.Printf("Found subscribed regexp: %s\n", topic)
			for i, c := range clients {
				log.Printf("Sending %d bytes to %s: 0x%08x\n", len(msg.Payload), topic, c)
				n, err := io.Copy(c.Conn, &msg)
				if err != nil {
					log.Printf("Error sending payload to client: %v\n", err)
					s.ClientSubscriptions[re] = removeClientSubscription(clients, i)
				}
				log.Printf("Sent %d bytes to client\n", n)
			}

		}
	}

	log.Printf("Handling server subscriptions\n")
	serverMsg := Message{
		Topic:   topic,
		Payload: payload,
	}
	s.handleSubscriptions(serverMsg)

	return nil
}

func (s *Server) handleSubscriptions(msg Message) error {
	log.Printf("Handling server subscriptions: %s -> %s\n", msg.Topic, msg.Payload)
	for re, handler := range s.ServerSubscriptions {
		log.Printf("Match: %t\n", re.Match([]byte(msg.Topic)))
		if re.MatchString(msg.Topic) {
			if err := handler(s, msg); err != nil {
				log.Errorf("ERROR: Error handling message: %s -> %s\n", msg.Topic, err.Error())
				return err
			}
		}
	}

	return nil
}

func (s *Server) HandleClient(conn net.Conn) {
	log.Printf("Client connected\n")

	c := &Client{
		Conn: conn,
	}

	run := true
	// run loop forever (or until ctrl-c)
	for run {
		// get message, output
		buffer := make([]byte, 16*1024)
		n, err := c.Conn.Read(buffer)

		if err != nil {
			log.Printf("Read error: %s\n", err.Error())
			break
		}

		if n == 0 {
			break
		}

		if log.Level == logrus.DebugLevel {
			debugMessage := fmt.Sprintf("Received(%d:%d) 0x%08x:", n, len(buffer), c)
			for i := 0; i < n; i++ {
				v := buffer[i]
				debugMessage += fmt.Sprintf("'%c':0x%02x ", uint8(v), uint8(v))
			}
		}
		for i := 0; i < n; {

			msg := Message{}

			//Parse the fixed header
			var fixed uint8
			fixed, i = getUint8(buffer, i)
			msg.PacketType = (fixed >> 4) & 0x0f
			msg.PacketFlags = (fixed) & 0x0f
			msg.PacketLength, i = GetVariableInteger(buffer, i)

			log.Debugf("Packet type: %d\n", msg.PacketType)
			log.Debugf("Packet length: %d, i:%d\n", msg.PacketLength.Int(), i)
			log.Debugf("Packet flags: %d\n", msg.PacketFlags)
			msg.Payload = buffer[i : i+msg.PacketLength.Int()]

			if log.Level == logrus.DebugLevel {

				debugMessage := fmt.Sprintf("Message Received(%d):", len(msg.Payload))
				for _, v := range msg.Payload {
					debugMessage += fmt.Sprintf("'%c':0x%02x ", uint8(v), uint8(v))
				}
			}

			handler, ok := s.handlers[int(msg.PacketType)]
			if !ok {
				panic(fmt.Errorf("Handler not found for command %d: %s", msg.PacketType, string(msg.Payload)))
			} else {
				if err := handler(s, c, msg); err != nil {
					log.Errorf("Error handling command: %s\n", err.Error())
					run = false
					break
				}
			}

			i += msg.PacketLength.Int()
		}
	}

	if c.Will != nil {
		log.Debugf("Client has will: %s -> %s\n", c.Will.Topic, c.Will.Payload)
		s.Publish(c.Will.Topic, c.Will.QOS, c.Will.Retain, c.Will.Payload)
	}

	//Remove subscribtions
	for re, clients := range s.ClientSubscriptions {
		for i, client := range clients {
			if client == c {
				s.ClientSubscriptions[re] = removeClientSubscription(clients, i)
				break
			}
		}
	}

	c.Conn.Close()
}

func removeClientSubscription(clients []*Client, i int) []*Client {
	log.Debugf("Removing client at index %d\n", i)
	clients[i] = clients[len(clients)-1]
	// We do not need to put s[i] at the end, as it will be discarded anyway
	return clients[:len(clients)-1]
}

func GetVariableInteger(buffer []uint8, i int) (VariableLengthInt, int) {
	vli, length := VariableLengthIntParse(buffer[i:])
	return vli, i + length
}

func (s *Server) Run() {
	var ln net.Listener
	var err error

	if s.tls != nil {
		ln, err = tls.Listen("tcp", fmt.Sprintf(":8883"), s.tls)
	} else {
		ln, err = net.Listen("tcp", fmt.Sprintf(":1883"))
	}
	if err != nil {
		panic(err)
	}

	log.Printf("Listening for connections\n")
	for s.run {
		// accept connection
		conn, _ := ln.Accept()
		go s.HandleClient(conn)

	}
}

type VariableLengthInt struct {
	Value uint64
}

func VariableLengthIntParse(buffer []uint8) (VariableLengthInt, int) {
	val := uint64(0)
	length := 0
	multiplier := uint64(1)
	for _, v := range buffer {
		raw := v & 0x7f
		val += uint64(raw) * multiplier
		length = length + 1
		log.Debugf("m: %d, v: %d, l: %d, raw: %d:0x%02x, val:%d\n", multiplier, v, length, raw, raw, val)

		if (v & 0x80) == 0 {
			//High bit not set means done
			break
		}

		multiplier *= 128

	}

	return VariableLengthInt{Value: val}, length
}

func (vli *VariableLengthInt) ToBytes() ([]uint8, int) {

	return []uint8{uint8(vli.Value)}, 1

}

func (vli *VariableLengthInt) Int() int {
	return int(vli.Value)
}

type Message struct {
	PacketType    uint8
	PacketFlags   uint8
	PacketLength  VariableLengthInt
	PacketId      uint16
	PacketOptions uint8
	Sent          bool

	Topic   string
	Payload []byte
	Qos     uint8
	Retain  uint8
}

type Index struct {
	index int
}

func (msg *Message) Read(telegram []byte) (int, error) {
	if msg.Sent {
		return 0, io.EOF
	}

	packet_length := msg.PacketLength.Int() + 2

	index := 0
	telegram[index] = (msg.PacketType&0x0F)<<4 | (msg.PacketFlags & 0x0F)
	index++

	vli, length := msg.PacketLength.ToBytes()
	for i := 0; i < length; i++ {
		telegram[index] = vli[i]
		index++
	}

	for i := 0; i < msg.PacketLength.Int(); i++ {
		telegram[index] = msg.Payload[i]
		index++
	}

	if log.Level == logrus.DebugLevel {
		debugMessage := fmt.Sprintf("Message reader(%d:%d): ", packet_length, index)
		for i := 0; i < packet_length; i++ {
			debugMessage += fmt.Sprintf("0x%02x ", telegram[i])
		}
		log.Debug(debugMessage)
	}

	msg.Sent = true
	return packet_length, nil

}

func NewServer(tls *tls.Config) *Server {

	s := &Server{
		tls: tls,
		run: true,
	}

	log.Level = logrus.WarnLevel

	s.handlers = make(map[int]mqttCommandPacketHandler, 16)
	s.handlers[1] = mqttCommandConnectHandler
	s.handlers[3] = mqttCommandPublishHandler
	s.handlers[6] = mqttCommandPubRelHandler
	s.handlers[8] = mqttCommandSubscribeHandler
	s.handlers[12] = mqttCommandPingRequestHandler
	s.handlers[14] = mqttCommandDisconnectHandler

	s.ServerSubscriptions = make(map[*regexp.Regexp]SubscriptionHandler)
	s.ClientSubscriptions = make(map[*regexp.Regexp][]*Client)
	return s
}

func (s *Server) SendConnAck(c *Client) error {

	msg := Message{
		PacketType:   2,
		PacketFlags:  0,
		PacketLength: VariableLengthInt{3},
		Sent:         false,
	}

	msg.Payload = []byte{0, 0, 0, 0}

	log.Printf("Sending ConnAck\n")
	_, err := io.Copy(c.Conn, &msg)

	return err
}

func (s *Server) SendPubAck(c *Client, msg Message) error {

	response := Message{
		PacketType:   4,
		PacketFlags:  0,
		PacketLength: VariableLengthInt{4},
		Sent:         false,
	}

	response.Payload = []byte{
		byte(msg.PacketId >> 8),
		byte(msg.PacketId & 0xFF),
		0,
		0,
	}

	log.Printf("Sending PubAck: %d\n", msg.Qos)
	_, err := io.Copy(c.Conn, &response)

	return err
}

func (s *Server) SendPubRec(c *Client, msg Message) error {

	response := Message{
		PacketType:   5,
		PacketFlags:  0,
		PacketLength: VariableLengthInt{4},
		Sent:         false,
	}

	response.Payload = []byte{
		byte(msg.PacketId >> 8),
		byte(msg.PacketId & 0xFF),
		0,
		0,
	}

	log.Printf("Sending PubRec: %d\n", msg.Qos)
	_, err := io.Copy(c.Conn, &response)

	return err
}

func (s *Server) SendPubComp(c *Client, msg Message) error {

	response := Message{
		PacketType:   7,
		PacketFlags:  0,
		PacketLength: VariableLengthInt{4},
		Sent:         false,
	}

	response.Payload = []byte{
		byte(msg.PacketId >> 8),
		byte(msg.PacketId & 0xFF),
		0,
		0,
	}

	log.Printf("Sending PubComp: %d\n", msg.Qos)
	_, err := io.Copy(c.Conn, &response)

	return err
}

func (s *Server) SendSubAck(c *Client, package_id uint16, qos uint8) error {

	msg := Message{
		PacketType:  9,
		PacketFlags: 0,
		Sent:        false,
	}

	msg.Payload = []byte{
		byte(package_id >> 8),
		byte(package_id & 0xFF),
		0,
		byte(qos),
	}

	msg.PacketLength = VariableLengthInt{uint64(len(msg.Payload))}

	log.Printf("Sending SubAck\n")
	_, err := io.Copy(c.Conn, &msg)

	return err
}

func (s *Server) SendPingResp(c *Client) error {

	msg := Message{
		PacketType:   13,
		PacketFlags:  0,
		PacketLength: VariableLengthInt{0},
		Sent:         false,
	}

	log.Printf("Sending PingResp\n")
	_, err := io.Copy(c.Conn, &msg)

	return err
}

func mqttCommandConnectHandler(s *Server, c *Client, msg Message) error {
	log.Printf("Connect request: ")

	if log.Level == logrus.DebugLevel {

		for i := 0; i < msg.PacketLength.Int(); i++ {
			fmt.Printf("'%c':0x%02x ", msg.Payload[i], msg.Payload[i])
		}
		fmt.Printf("\n")
	}

	var packet ConnectPacket
	index := 0

	packet.ProtocolName, index = getString(msg.Payload, index)
	packet.ProtocolLevel, index = getUint8(msg.Payload, index)
	packet.ConnectFlags, index = getUint8(msg.Payload, index)
	packet.KeepAlive, index = getUint16(msg.Payload, index)
	if packet.ProtocolLevel == 5 {
		packet.PropertiesLength, index = getUint8(msg.Payload, index)
		log.Printf("Properties: %d\n", packet.PropertiesLength)

		if packet.PropertiesLength > 0 {
			packet.Properties = make([]uint8, packet.PropertiesLength)
			for i := uint8(0); i < packet.PropertiesLength; i++ {
				packet.Properties[i], index = getUint8(msg.Payload, index)
			}
		}

	}

	log.Printf("Index 1: %d\n", index)
	packet.ClientId, index = getString(msg.Payload, index)

	log.Printf("Client id: %s\n", packet.ClientId)
	log.Print("Control packet: ", packet)
	log.Printf("Index: %d\n", index)

	clean_start := ((packet.ConnectFlags >> 1) & 0x01) == 1
	has_will := ((packet.ConnectFlags >> 2) & 0x01) == 1
	will_qos := (packet.ConnectFlags >> 3) & 0x03
	will_retain := ((packet.ConnectFlags >> 5) & 0x01) == 1
	has_password := ((packet.ConnectFlags >> 6) & 0x01) == 1
	has_username := ((packet.ConnectFlags >> 7) & 0x01) == 1

	log.Printf("Clean start: %t\n", clean_start)
	log.Printf("Will: %t\n", has_will)
	if has_will {
		log.Printf("Will QOS: %d\n", will_qos)
		log.Printf("Will retain: %t\n", will_retain)
	}

	log.Printf("Has password: %t\n", has_password)
	log.Printf("Has username: %t\n", has_username)

	c.ProtocolLevel = packet.ProtocolLevel

	if has_will {
		will := Will{}
		//First comes number of will properties
		will.NumProperties, index = GetVariableInteger(msg.Payload, index)

		if will.NumProperties.Int() != 0 {
			log.Fatalf("Will properties not handled yet\n")
		}
		//Next comes will topic
		will.Topic, index = getString(msg.Payload, index)

		//Payload last
		will.Payload, index = getBytes(msg.Payload, index)

		will.QOS = int(will_qos)
		will.Retain = will_retain

		c.Will = &will

		log.Printf("Will(%d): %s -> %s\n", will.NumProperties.Int(), will.Topic, will.Payload)

	}
	log.Print("Remaining: ", msg.Payload[index:])

	return s.SendConnAck(c)

}

func mqttCommandPublishHandler(s *Server, c *Client, msg Message) error {
	index := 0
	msg.Retain = (msg.PacketFlags & 0x01)
	msg.Qos = (msg.PacketFlags >> 1) & 0x3

	msg.Topic, index = getString(msg.Payload, index)
	msg.PacketId, index = getUint16(msg.Payload, index)

	var properties uint8
	if c.ProtocolLevel == 5 {
		properties, index = getUint8(msg.Payload, index)
		log.Printf("Pubish properties: %d\n", properties)
	}

	msg.Payload = msg.Payload[index:]

	log.Printf("Publish: Topic: %s, id: %d, props: %d\n",
		msg.Topic,
		msg.PacketId,
		properties)

	s.handleSubscriptions(msg)

	if msg.Qos == 2 {
		return s.SendPubRec(c, msg)
	} else {
		return s.SendPubAck(c, msg)
	}
}

func mqttCommandPubRelHandler(s *Server, c *Client, msg Message) error {

	log.Print("PubRel: ", msg.Payload)
	msg.PacketId, _ = getUint16(msg.Payload, 0)

	if msg.PacketLength.Int() == 4 {
		reason := msg.Payload[2]
		properties := msg.Payload[3]

		log.Printf("Publish: Topic: %s, id: %d, props: %d, reason: %d\n",
			msg.Topic,
			msg.PacketId,
			properties,
			reason)
	}

	return s.SendPubComp(c, msg)
}

func mqttCommandDisconnectHandler(s *Server, c *Client, msg Message) error {
	c.Conn.Close()
	return fmt.Errorf("Client requested disconnect")
}

func mqttCommandSubscribeHandler(s *Server, c *Client, msg Message) error {
	index := 0

	//Variable length header
	msg.PacketId, index = getUint16(msg.Payload, index)

	if c.ProtocolLevel >= 5 {
		msg.PacketOptions, index = getUint8(msg.Payload, index)
	}

	sub_topic, index := getString(msg.Payload, index)
	sub_options, index := getUint8(msg.Payload, index)

	re := getTopicRegEx(sub_topic)

	log.Printf("Subscribe c: 0x%08x\n", c)
	s.ClientSubscriptions[re] = append(s.ClientSubscriptions[re], c)

	qos := sub_options & 0x03

	log.Printf("Sub request: %d -> 0x%02x -> %s \n", msg.PacketId, sub_options, sub_topic)

	return s.SendSubAck(c, msg.PacketId, qos)
}

func mqttCommandPingRequestHandler(s *Server, c *Client, msg Message) error {

	return s.SendPingResp(c)
}

func getUint16(buffer []byte, index int) (uint16, int) {
	value := uint16(buffer[index])<<8 | uint16(buffer[index+1])

	return value, index + 2
}

func getUint8(buffer []byte, index int) (uint8, int) {
	value := uint8(buffer[index])

	return value, index + 1
}

func getBytes(buffer []byte, index int) ([]byte, int) {

	//First get length
	length, index := getUint16(buffer, index)

	log.Printf("L:%d, i:%d\n", length, index)

	//Now get the value
	value := buffer[index : index+int(length)]

	return value, index + int(length)
}

func getString(buffer []byte, index int) (string, int) {
	value, index := getBytes(buffer, index)
	return string(value), index
}

func getTopicRegEx(topic string) *regexp.Regexp {
	exp := strings.Replace(topic, "+", "([^/\\n]*)", -1)
	log.Printf("Regexp: %s\n", exp)
	re := regexp.MustCompile(exp)

	return re
}
