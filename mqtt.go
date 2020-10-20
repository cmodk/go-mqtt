package mqtt

import (
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"regexp"
	"strings"
)

type mqttCommandPacketHandler func(*Server, net.Conn, Message) error
type SubscriptionHandler func(*Server, Message) error

type Server struct {
	run      bool
	tls      *tls.Config
	handlers map[int]mqttCommandPacketHandler

	ServerSubscriptions map[*regexp.Regexp]SubscriptionHandler
}

func (s *Server) Subscribe(topic string, qos int, handler SubscriptionHandler) error {

	exp := strings.Replace(topic, "+", "([^/\\n]*)", -1)
	log.Printf("Regexp: %s\n", exp)
	re := regexp.MustCompile(exp)

	s.ServerSubscriptions[re] = handler

	return nil
}

func (s *Server) Publish(topic string, qos int, retain bool, payload []byte) error {

	return nil
}

func (s *Server) HandleClient(conn net.Conn) {
	log.Printf("Client connected\n")

	run := true
	// run loop forever (or until ctrl-c)
	for run {
		// get message, output
		buffer := make([]byte, 16*1024)
		n, err := conn.Read(buffer)

		if err != nil {
			log.Printf("Read error: %s\n", err.Error())
			break
		}

		if n == 0 {
			break
		}

		fmt.Printf("Received(%d:%d):", n, len(buffer))
		for i := 0; i < n; i++ {
			v := buffer[i]
			fmt.Printf("'%c':0x%02x ", uint8(v), uint8(v))
		}
		fmt.Printf("\n")
		for i := 0; i < n; {

			msg := Message{}

			//Parse the fixed header
			msg.PacketType = (buffer[i] >> 4) & 0x0f
			msg.PacketFlags = (buffer[i]) & 0x0f
			msg.PacketLength = buffer[i+1]

			packet_buffer_length := 2 + int(msg.PacketLength)
			log.Printf("Packet type: %d\n", msg.PacketType)
			log.Printf("Packet length: %d\n", msg.PacketLength)
			log.Printf("Packet flags: %d\n", msg.PacketFlags)
			msg.Payload = buffer[i+2 : i+packet_buffer_length]
			fmt.Printf("Message Received(%d):", len(msg.Payload))
			for _, v := range msg.Payload {
				fmt.Printf("'%c':0x%02x ", uint8(v), uint8(v))
			}
			fmt.Printf("\n")

			handler, ok := s.handlers[int(msg.PacketType)]
			if !ok {
				panic(fmt.Errorf("Handler not found for command %d: %s", msg.PacketType, string(msg.Payload)))
			} else {
				if err := handler(s, conn, msg); err != nil {
					log.Printf("Error handling command: %s\n", err.Error())
					run = false
					break
				}
			}

			i += packet_buffer_length
		}
	}
	conn.Close()
}

func (s *Server) Run() {
	var ln net.Listener
	var err error

	log.Printf("tls: 0x%08x\n", s.tls)
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

type Message struct {
	PacketType    uint8
	PacketFlags   uint8
	PacketLength  uint8
	PacketId      uint16
	PacketOptions uint8
	Sent          bool

	Topic   string
	Payload []byte
	Qos     uint8
	Retain  uint8
}

func (msg *Message) Read(telegram []byte) (int, error) {
	if msg.Sent {
		return 0, io.EOF
	}

	packet_length := int(msg.PacketLength) + 2

	telegram[0] = (msg.PacketType&0x0F)<<4 | (msg.PacketFlags & 0x0F)
	telegram[1] = msg.PacketLength

	for i := 0; i < len(msg.Payload); i++ {
		telegram[2+i] = msg.Payload[i]
	}

	log.Printf("Message reader(%d): ", packet_length)
	for i := 0; i < packet_length; i++ {
		fmt.Printf("0x%02x ", telegram[i])
	}
	fmt.Printf("\n\n")

	msg.Sent = true
	return packet_length, nil

}

func NewServer(tls *tls.Config) *Server {

	s := &Server{
		tls: tls,
		run: true,
	}

	s.handlers = make(map[int]mqttCommandPacketHandler, 16)
	s.handlers[1] = mqttCommandConnectHandler
	s.handlers[3] = mqttCommandPublishHandler
	s.handlers[6] = mqttCommandPubRelHandler
	s.handlers[8] = mqttCommandSubscribeHandler
	s.handlers[12] = mqttCommandPingRequestHandler
	s.handlers[14] = mqttCommandDisconnectHandler

	s.ServerSubscriptions = make(map[*regexp.Regexp]SubscriptionHandler)
	return s
}

func (s *Server) SendConnAck(conn net.Conn) error {

	msg := Message{
		PacketType:   2,
		PacketFlags:  0,
		PacketLength: 3,
		Sent:         false,
	}

	msg.Payload = []byte{0, 0, 0, 0}

	log.Printf("Sending ConnAck\n")
	_, err := io.Copy(conn, &msg)

	return err
}

func (s *Server) SendPubAck(conn net.Conn, msg Message) error {

	response := Message{
		PacketType:   4,
		PacketFlags:  0,
		PacketLength: 4,
		Sent:         false,
	}

	response.Payload = []byte{
		byte(msg.PacketId >> 8),
		byte(msg.PacketId & 0xFF),
		0,
		0,
	}

	log.Printf("Sending PubAck: %d\n", msg.Qos)
	_, err := io.Copy(conn, &response)

	return err
}

func (s *Server) SendPubRec(conn net.Conn, msg Message) error {

	response := Message{
		PacketType:   5,
		PacketFlags:  0,
		PacketLength: 4,
		Sent:         false,
	}

	response.Payload = []byte{
		byte(msg.PacketId >> 8),
		byte(msg.PacketId & 0xFF),
		0,
		0,
	}

	log.Printf("Sending PubRec: %d\n", msg.Qos)
	_, err := io.Copy(conn, &response)

	return err
}

func (s *Server) SendPubComp(conn net.Conn, msg Message) error {

	response := Message{
		PacketType:   7,
		PacketFlags:  0,
		PacketLength: 4,
		Sent:         false,
	}

	response.Payload = []byte{
		byte(msg.PacketId >> 8),
		byte(msg.PacketId & 0xFF),
		0,
		0,
	}

	log.Printf("Sending PubComp: %d\n", msg.Qos)
	_, err := io.Copy(conn, &response)

	return err
}

func (s *Server) SendSubAck(conn net.Conn, package_id uint16, qos uint8) error {

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

	msg.PacketLength = uint8(len(msg.Payload))

	log.Printf("Sending SubAck\n")
	_, err := io.Copy(conn, &msg)

	return err
}

func (s *Server) SendPingResp(conn net.Conn) error {

	msg := Message{
		PacketType:   13,
		PacketFlags:  0,
		PacketLength: 0,
		Sent:         false,
	}

	log.Printf("Sending PingResp\n")
	_, err := io.Copy(conn, &msg)

	return err
}

func mqttCommandConnectHandler(s *Server, conn net.Conn, msg Message) error {
	log.Printf("Connect request: ")

	for i := 0; i < int(msg.PacketLength); i++ {
		fmt.Printf("'%c':0x%02x ", msg.Payload[i], msg.Payload[i])
	}
	fmt.Printf("\n")

	return s.SendConnAck(conn)

}

func mqttCommandPublishHandler(s *Server, conn net.Conn, msg Message) error {

	msg.Retain = (msg.PacketFlags & 0x01)
	msg.Qos = (msg.PacketFlags >> 1) & 0x3

	length := getUint16(msg.Payload)
	log.Printf("Length: %d\n", length)
	msg.Topic = string(msg.Payload[2 : length+2])
	msg.PacketId = getUint16(msg.Payload[length+2:])
	properties := msg.Payload[length+4]

	msg.Payload = msg.Payload[length+5:]

	log.Printf("Publish: Length: %d, Topic: %s, id: %d, props: %d\n",
		length,
		msg.Topic,
		msg.PacketId,
		properties)

	for re, handler := range s.ServerSubscriptions {
		log.Printf("Match: %t\n", re.Match([]byte(msg.Topic)))
		if re.MatchString(msg.Topic) {
			if err := handler(s, msg); err != nil {
				panic(err)
			}
		}
	}
	if msg.Qos == 2 {
		return s.SendPubRec(conn, msg)
	} else {
		return s.SendPubAck(conn, msg)
	}
}

func mqttCommandPubRelHandler(s *Server, conn net.Conn, msg Message) error {

	log.Print("PubRel: ", msg.Payload)
	msg.PacketId = getUint16(msg.Payload)

	if msg.PacketLength == 4 {
		reason := msg.Payload[2]
		properties := msg.Payload[3]

		log.Printf("Publish: Topic: %s, id: %d, props: %d, reason: %d\n",
			msg.Topic,
			msg.PacketId,
			properties,
			reason)
	}

	return s.SendPubComp(conn, msg)
}

func mqttCommandDisconnectHandler(s *Server, conn net.Conn, msg Message) error {
	conn.Close()
	return fmt.Errorf("Client requested disconnect")
}

func mqttCommandSubscribeHandler(s *Server, conn net.Conn, msg Message) error {

	//Variable length header
	msg.PacketId = uint16(msg.Payload[0])<<8 | uint16(msg.Payload[1])
	msg.PacketOptions = msg.Payload[2]

	sub_request := msg.Payload[3:]
	log.Print(msg.Payload)
	log.Print(sub_request)
	sub_length := getUint16(sub_request)
	sub_topic := string(sub_request[2 : sub_length+2])
	sub_options := sub_request[sub_length+2]

	qos := sub_options & 0x03

	log.Printf("Sub request: %d -> %d -> 0x%02x -> %s \n", msg.PacketId, sub_length, sub_options, sub_topic)

	return s.SendSubAck(conn, msg.PacketId, qos)
}

func mqttCommandPingRequestHandler(s *Server, conn net.Conn, msg Message) error {

	return s.SendPingResp(conn)
}

func getUint16(buffer []byte) uint16 {
	return uint16(buffer[0])<<8 | uint16(buffer[1])
}
