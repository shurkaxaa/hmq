package broker

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/alexandercampbell-wf/matchbox"
	simplejson "github.com/bitly/go-simplejson"
	"github.com/shurkaxaa/hmq/broker/lib/sessions"
	"github.com/shurkaxaa/hmq/broker/lib/topics"
	"github.com/shurkaxaa/hmq/plugins/bridge"
	"golang.org/x/net/websocket"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"go.uber.org/zap"
)

const (
	// special pub topic for cluster info BrokerInfoTopic
	BrokerInfoTopic = "broker000100101info"

	// connection ytacking
	ConntrackTopic    = "$SYS/broker/connection/clients"
	ConntrackSubTopic = ConntrackTopic + "/+"

	// CLIENT is an end user.
	CLIENT = 0
	// ROUTER is another router in the cluster.
	ROUTER = 1
	//REMOTE is the router connect to other cluster
	REMOTE  = 2
	CLUSTER = 3
)

const (
	_GroupTopicRegexp = `^\$share/([0-9a-zA-Z_-]+)/(.*)$`
)

const (
	Connected    = 1
	Disconnected = 2
)

var (
	groupCompile = regexp.MustCompile(_GroupTopicRegexp)
)

type client struct {
	typ         int
	broker      *Broker
	conn        net.Conn
	info        info
	route       route
	status      int
	ctx         context.Context
	cancelFunc  context.CancelFunc
	session     *sessions.Session
	subMap      map[string]*subscription
	topicsMgr   *topics.Manager
	subs        []matchbox.Subscriber
	qoss        []byte
	rmsgs       []*packets.PublishPacket
	routeSubMap map[string]uint64
}

type subscription struct {
	client    *client
	topic     string
	qos       byte
	share     bool
	groupName string
	id        string
}

func (this *subscription) ID() string {
	return this.id
}

type info struct {
	clientID  string
	username  string
	password  []byte
	keepalive uint16
	willMsg   *packets.PublishPacket
	localIP   string
	remoteIP  string
	authMeta  interface{}
}

type route struct {
	remoteID  string
	remoteUrl string
}

var (
	DisconnectedPacket = packets.NewControlPacket(packets.Disconnect).(*packets.DisconnectPacket)
	r                  = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func (c *client) init() {
	c.status = Connected
	c.info.localIP, _, _ = net.SplitHostPort(c.conn.LocalAddr().String())
	remoteAddr := c.conn.RemoteAddr()
	remoteNetwork := remoteAddr.Network()
	c.info.remoteIP = ""
	if remoteNetwork != "websocket" {
		c.info.remoteIP, _, _ = net.SplitHostPort(remoteAddr.String())
	} else {
		ws := c.conn.(*websocket.Conn)
		c.info.remoteIP, _, _ = net.SplitHostPort(ws.Request().RemoteAddr)
	}
	c.ctx, c.cancelFunc = context.WithCancel(context.Background())
	c.subMap = make(map[string]*subscription)
	c.topicsMgr = c.broker.topicsMgr
	c.routeSubMap = make(map[string]uint64)
}

func (c *client) readLoop() {
	nc := c.conn
	b := c.broker
	if nc == nil || b == nil {
		return
	}

	keepAlive := time.Second * time.Duration(c.info.keepalive)
	timeOut := keepAlive + (keepAlive / 2)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			//add read timeout
			if keepAlive > 0 {
				if err := nc.SetReadDeadline(time.Now().Add(timeOut)); err != nil {
					log.Error("set read timeout error: ", zap.Error(err), zap.String("ClientID", c.info.clientID))
					msg := &Message{
						client: c,
						packet: DisconnectedPacket,
					}
					b.SubmitWork(c.info.clientID, msg)
					return
				}
			}

			packet, err := packets.ReadPacket(nc)
			if err != nil {
				log.Info("read packet error: ", zap.Error(err), zap.String("ClientID", c.info.clientID))
				msg := &Message{
					client: c,
					packet: DisconnectedPacket,
				}
				b.SubmitWork(c.info.clientID, msg)
				return
			}
			log.Debug("new packet from client:", zap.String("ClientID", c.info.clientID))

			msg := &Message{
				client: c,
				packet: packet,
			}
			b.SubmitWork(c.info.clientID, msg)
		}
	}

}

func ProcessMessage(msg *Message) {
	c := msg.client
	ca := msg.packet
	if ca == nil {
		return
	}

	if c.typ == CLIENT {
		log.Debug("Recv message:", zap.String("message type", reflect.TypeOf(msg.packet).String()[9:]), zap.String("ClientID", c.info.clientID))
	}

	switch ca.(type) {
	case *packets.ConnackPacket:
		/*
		 * In case of REMOTE (connect to other router) subscribe to $SYS/broker/connection/clients/" + clientID
		 * We need to handle remote connected events at least to drop local connections with the same clientID
		 */
		switch c.typ {
		case REMOTE:
			packet := ca.(*packets.ConnackPacket)
			log.Debug("ConnackPacket packet from REMOTE", zap.Int("ReturnCode", int(packet.ReturnCode)))
			if int(packet.ReturnCode) != packets.Accepted {
				break
			}
			subInfo := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
			subInfo.Topics = append(subInfo.Topics, ConntrackSubTopic)
			subInfo.Qoss = append(subInfo.Qoss, byte(topics.QosAtMostOnce))
			if len(subInfo.Topics) > 0 {
				err := c.WriterPacket(subInfo)
				if err != nil {
					log.Error("Can not subscribe to remote connection tracking:", zap.Error(err))
				}
			}
		}
	case *packets.ConnectPacket:
	case *packets.PublishPacket:
		packet := ca.(*packets.PublishPacket)
		c.ProcessPublish(packet)
	case *packets.PubackPacket:
	case *packets.PubrecPacket:
	case *packets.PubrelPacket:
	case *packets.PubcompPacket:
	case *packets.SubscribePacket:
		packet := ca.(*packets.SubscribePacket)
		c.ProcessSubscribe(packet)
	case *packets.SubackPacket:
	case *packets.UnsubscribePacket:
		packet := ca.(*packets.UnsubscribePacket)
		c.ProcessUnSubscribe(packet)
	case *packets.UnsubackPacket:
	case *packets.PingreqPacket:
		c.ProcessPing()
	case *packets.PingrespPacket:
	case *packets.DisconnectPacket:
		c.Close()
	default:
		log.Info("Recv Unknow message.......", zap.String("ClientID", c.info.clientID))
	}
}

func (c *client) ProcessPublish(packet *packets.PublishPacket) {
	switch c.typ {
	case CLIENT:
		c.processClientPublish(packet)
	case ROUTER:
		c.processRouterPublish(packet)
	case CLUSTER:
		c.processClusterPublish(packet)
	case REMOTE:
		c.processRemotePublish(packet)
	}

}

func (c *client) processRemotePublish(packet *packets.PublishPacket) {
	log.Debug("Publish from remote")
	topic := packet.TopicName
	if !strings.HasPrefix(topic, ConntrackTopic) {
		return
	}
	// Handle remote conntrack packet (broker.OnlineOfflineNotification)
	// Close local connections with the same clientID if any
	// packet.Payload = []byte(fmt.Sprintf(`{"clientID":"%s","online":%v,"timestamp":"%s"}`, clientID, online, time.Now().UTC().Format(time.RFC3339)))
	js, err := simplejson.NewJson(packet.Payload)
	if err != nil {
		log.Warn("parse info message err", zap.Error(err))
		return
	}
	clientID, err := js.Get("clientID").String()
	if err != nil {
		return
	}
	if clientID == "" {
		log.Debug("No clientID found in conntrack message, ", zap.String("msg", string(packet.Payload)))
		return
	}
	online, err := js.Get("online").Bool()
	if err != nil || !online {
		return
	}
	old, exist := c.broker.clients.Load(clientID)
	if exist {
		log.Warn("Local client exist, disconnecting ...", zap.String("clientID", c.info.clientID))
		ol, ok := old.(*client)
		if ok {
			ol.Close()
		}
	}
}

func (c *client) processClusterPublish(packet *packets.PublishPacket) {
	log.Debug("Publish from cluster")
	if c.status == Disconnected {
		return
	}

	topic := packet.TopicName
	if topic == BrokerInfoTopic {
		c.ProcessInfo(packet)
		return
	}

}

func (c *client) processRouterPublish(packet *packets.PublishPacket) {
	log.Debug("Publish from router")
	if c.status == Disconnected {
		return
	}

	switch packet.Qos {
	case QosAtMostOnce:
		c.ProcessPublishMessage(packet)
	case QosAtLeastOnce:
		puback := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		puback.MessageID = packet.MessageID
		if err := c.WriterPacket(puback); err != nil {
			log.Error("send puback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			return
		}
		c.ProcessPublishMessage(packet)
	case QosExactlyOnce:
		return
	default:
		log.Error("publish with unknown qos", zap.String("ClientID", c.info.clientID))
		return
	}

}

func (c *client) processClientPublish(packet *packets.PublishPacket) {
	log.Debug("Publish from client")

	topic := packet.TopicName

	if !c.broker.CheckTopicAuth(PUB, c.info.clientID, c.info.username, c.info.remoteIP, topic) {
		log.Error("Pub Topics Auth failed, ", zap.String("topic", topic), zap.String("ClientID", c.info.clientID))
		return
	}

	if c.broker.hooks != nil && !c.broker.hooks.Publish(c.broker, packet, c.info.authMeta) {
		log.Error("Pub Topics hook failed, ", zap.String("topic", topic), zap.String("ClientID", c.info.clientID))
		c.Close()
		return
	}

	//publish kafka
	c.broker.Publish(&bridge.Elements{
		ClientID:  c.info.clientID,
		Username:  c.info.username,
		Action:    bridge.Publish,
		Timestamp: time.Now().Unix(),
		Payload:   string(packet.Payload),
		Topic:     topic,
	})

	switch packet.Qos {
	case QosAtMostOnce:
		c.ProcessPublishMessage(packet)
	case QosAtLeastOnce:
		puback := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		puback.MessageID = packet.MessageID
		if err := c.WriterPacket(puback); err != nil {
			log.Error("send puback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			return
		}
		c.ProcessPublishMessage(packet)
	case QosExactlyOnce:
		return
	default:
		log.Error("publish with unknown qos", zap.String("ClientID", c.info.clientID))
		return
	}

}

func (c *client) ProcessPublishMessage(packet *packets.PublishPacket) {

	b := c.broker
	if b == nil {
		return
	}
	typ := c.typ

	if packet.Retain {
		if err := c.topicsMgr.Retain(packet); err != nil {
			log.Error("Error retaining message: ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		}
	}

	err := c.topicsMgr.Subscribers([]byte(packet.TopicName), packet.Qos, &c.subs, &c.qoss)
	if err != nil {
		log.Error("Error retrieving subscribers list: ", zap.String("ClientID", c.info.clientID))
		return
	}

	// fmt.Println("psubs num: ", len(c.subs))
	if len(c.subs) == 0 {
		return
	}

	var qsub []int
	for i, sub := range c.subs {
		s, ok := sub.(*subscription)
		if ok {
			log.Debug("Check subscriber: ",
				zap.String("brokerID", s.client.broker.id),
				zap.String("Client ID", c.info.clientID),
				zap.Int("Client type", typ),
				zap.String("Subscriber clientID", s.client.info.clientID),
				zap.Int("Subscriber client type", s.client.typ))
			if s.client.typ == ROUTER {
				if typ != CLIENT {
					continue
				}
			}

			if typ == ROUTER && s.client.typ != CLIENT {
				// we should not re-route, only pubish to local subscribers
				continue
			}

			if typ == CLIENT {
				if s.client.typ != CLIENT && s.client.typ != REMOTE {
					// client can only publish to local clients and remote subscribers
					continue
				}
			}

			if s.share {
				qsub = append(qsub, i)
			} else {
				publish(s, packet)
			}

		}

	}

	if len(qsub) > 0 {
		idx := r.Intn(len(qsub))
		sub := c.subs[qsub[idx]].(*subscription)
		publish(sub, packet)
	}

}

func (c *client) ProcessSubscribe(packet *packets.SubscribePacket) {
	switch c.typ {
	case CLIENT:
		c.processClientSubscribe(packet)
	case ROUTER:
		fallthrough
	case REMOTE:
		c.processRouterSubscribe(packet)
	}
}

func (c *client) processClientSubscribe(packet *packets.SubscribePacket) {
	if c.status == Disconnected {
		return
	}

	b := c.broker
	if b == nil {
		return
	}
	topics := packet.Topics
	qoss := packet.Qoss

	suback := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	suback.MessageID = packet.MessageID
	var retcodes []byte

	for i, topic := range topics {
		t := topic
		//check topic auth for client
		if !b.CheckTopicAuth(SUB, c.info.clientID, c.info.username, c.info.remoteIP, topic) {
			log.Error("Sub topic Auth failed: ", zap.String("topic", topic), zap.String("ClientID", c.info.clientID))
			retcodes = append(retcodes, QosFailure)
			continue
		}

		if c.broker.hooks != nil && !c.broker.hooks.Subscribe(c.broker, packet, c.info.authMeta) {
			log.Error("Sub topic Auth failed: ", zap.String("topic", topic), zap.String("ClientID", c.info.clientID))
			retcodes = append(retcodes, QosFailure)
			continue
		}

		b.Publish(&bridge.Elements{
			ClientID:  c.info.clientID,
			Username:  c.info.username,
			Action:    bridge.Subscribe,
			Timestamp: time.Now().Unix(),
			Topic:     topic,
		})

		groupName := ""
		share := false
		if strings.HasPrefix(topic, "$share/") {
			substr := groupCompile.FindStringSubmatch(topic)
			if len(substr) != 3 {
				retcodes = append(retcodes, QosFailure)
				continue
			}
			share = true
			groupName = substr[1]
			topic = substr[2]
		}

		if oldSub, exist := c.subMap[t]; exist {
			c.topicsMgr.Unsubscribe([]byte(oldSub.topic), oldSub)
			delete(c.subMap, t)
		}

		sub := &subscription{
			topic:     topic,
			qos:       qoss[i],
			client:    c,
			share:     share,
			groupName: groupName,
			id:        c.info.clientID + topic,
		}

		rqos, err := c.topicsMgr.Subscribe([]byte(topic), qoss[i], sub)
		if err != nil {
			log.Error("subscribe error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			retcodes = append(retcodes, QosFailure)
			continue
		}

		c.subMap[t] = sub

		c.session.AddTopic(t, qoss[i])
		retcodes = append(retcodes, rqos)
		c.topicsMgr.Retained([]byte(topic), &c.rmsgs)

	}

	suback.ReturnCodes = retcodes

	err := c.WriterPacket(suback)
	if err != nil {
		log.Error("send suback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		return
	}
	//broadcast subscribe message
	go b.BroadcastSubOrUnsubMessage(packet)

	//process retain message
	for _, rm := range c.rmsgs {
		if err := c.WriterPacket(rm); err != nil {
			log.Error("Error publishing retained message:", zap.Any("err", err), zap.String("ClientID", c.info.clientID))
		} else {
			log.Info("process retain  message: ", zap.Any("packet", packet), zap.String("ClientID", c.info.clientID))
		}
	}
}

func (c *client) processRouterSubscribe(packet *packets.SubscribePacket) {
	if c.status == Disconnected {
		return
	}

	b := c.broker
	if b == nil {
		return
	}
	topics := packet.Topics
	qoss := packet.Qoss

	suback := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	suback.MessageID = packet.MessageID
	var retcodes []byte

	for i, topic := range topics {
		t := topic
		groupName := ""
		share := false
		if strings.HasPrefix(topic, "$share/") {
			substr := groupCompile.FindStringSubmatch(topic)
			if len(substr) != 3 {
				retcodes = append(retcodes, QosFailure)
				continue
			}
			share = true
			groupName = substr[1]
			topic = substr[2]
		}

		sub := &subscription{
			topic:     topic,
			qos:       qoss[i],
			client:    c,
			share:     share,
			groupName: groupName,
			id:        c.info.clientID + topic,
		}

		rqos, err := c.topicsMgr.Subscribe([]byte(topic), qoss[i], sub)
		if err != nil {
			log.Error("subscribe error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			retcodes = append(retcodes, QosFailure)
			continue
		}

		c.subMap[t] = sub
		addSubMap(c.routeSubMap, topic)
		retcodes = append(retcodes, rqos)
	}

	suback.ReturnCodes = retcodes

	err := c.WriterPacket(suback)
	if err != nil {
		log.Error("send suback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		return
	}
}

func (c *client) ProcessUnSubscribe(packet *packets.UnsubscribePacket) {
	switch c.typ {
	case CLIENT:
		c.processClientUnSubscribe(packet)
	case ROUTER:
		c.processRouterUnSubscribe(packet)
	}
}

func (c *client) processRouterUnSubscribe(packet *packets.UnsubscribePacket) {
	if c.status == Disconnected {
		return
	}
	b := c.broker
	if b == nil {
		return
	}
	topics := packet.Topics

	for _, topic := range topics {
		sub, exist := c.subMap[topic]
		if exist {
			retainNum := delSubMap(c.routeSubMap, topic)
			if retainNum > 0 {
				continue
			}

			c.topicsMgr.Unsubscribe([]byte(sub.topic), sub)
			delete(c.subMap, topic)
		}

	}

	unsuback := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
	unsuback.MessageID = packet.MessageID

	err := c.WriterPacket(unsuback)
	if err != nil {
		log.Error("send unsuback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		return
	}
}

func (c *client) processClientUnSubscribe(packet *packets.UnsubscribePacket) {
	if c.status == Disconnected {
		return
	}
	b := c.broker
	if b == nil {
		return
	}
	topics := packet.Topics

	for _, topic := range topics {
		{
			//publish kafka

			b.Publish(&bridge.Elements{
				ClientID:  c.info.clientID,
				Username:  c.info.username,
				Action:    bridge.Unsubscribe,
				Timestamp: time.Now().Unix(),
				Topic:     topic,
			})

		}

		sub, exist := c.subMap[topic]
		if exist {
			c.topicsMgr.Unsubscribe([]byte(sub.topic), sub)
			c.session.RemoveTopic(topic)
			delete(c.subMap, topic)
		}

	}

	unsuback := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
	unsuback.MessageID = packet.MessageID

	err := c.WriterPacket(unsuback)
	if err != nil {
		log.Error("send unsuback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		return
	}
	// //process ubsubscribe message
	b.BroadcastSubOrUnsubMessage(packet)
}

func (c *client) ProcessPing() {
	if c.status == Disconnected {
		return
	}
	resp := packets.NewControlPacket(packets.Pingresp).(*packets.PingrespPacket)
	err := c.WriterPacket(resp)
	if err != nil {
		log.Error("send PingResponse error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		return
	}
}

func (c *client) Close() {
	if c.status == Disconnected {
		return
	}

	c.cancelFunc()

	c.status = Disconnected
	//wait for message complete
	// time.Sleep(1 * time.Second)
	// c.status = Disconnected

	b := c.broker
	b.Publish(&bridge.Elements{
		ClientID:  c.info.clientID,
		Username:  c.info.username,
		Action:    bridge.Disconnect,
		Timestamp: time.Now().Unix(),
	})

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	subs := c.subMap

	if b != nil {
		b.removeClient(c)
		for _, sub := range subs {
			err := b.topicsMgr.Unsubscribe([]byte(sub.topic), sub)
			if err != nil {
				log.Error("unsubscribe error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			}
		}

		if c.typ == CLIENT {
			b.BroadcastUnSubscribe(subs)
			//offline notification
			b.OnlineOfflineNotification(c.info.clientID, false)
			if b.hooks != nil {
				b.hooks.Disconnected(c.info.authMeta, c.session)
			}
		}

		if c.info.willMsg != nil {
			b.PublishMessage(c.info.willMsg)
		}

		if c.typ == CLUSTER {
			b.ConnectToDiscovery()
		}

		//do reconnect
		if c.typ == REMOTE {
			go b.connectRouter(c.route.remoteID, c.route.remoteUrl)
		}
	}
}

func (c *client) WriterPacket(packet packets.ControlPacket) error {
	log.Debug("Write packet start", zap.String("clientID", c.info.clientID))
	defer func() {
		if err := recover(); err != nil {
			log.Error("recover error, ", zap.Any("recover", r))
		}
	}()
	if c.status == Disconnected {
		log.Debug("Write packet end - Disconnected", zap.String("clientID", c.info.clientID))
		return nil
	}

	if packet == nil {
		log.Debug("Write packet end - packet == nil", zap.String("clientID", c.info.clientID))
		return nil
	}
	if c.conn == nil {
		log.Debug("Write packet end - connect lost ...", zap.String("clientID", c.info.clientID))
		c.Close()
		return errors.New("connect lost ....")
	}

	// we do not need mutex here
	// https://golang.org/pkg/net/#Conn
	// ... Multiple goroutines may invoke methods on a Conn simultaneously ...
	log.Debug("Write packet start - writing", zap.String("clientID", c.info.clientID))

	// TODO we need SetWriteDeadline to not lock forever? Why keep alive does not drop stalled connection?
	timeOut := time.Second * time.Duration(c.info.keepalive)
	if err := c.conn.SetWriteDeadline(time.Now().Add(timeOut)); err != nil {
		log.Error("set write timeout error: ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		return err
	}
	err := packet.Write(c.conn)
	log.Debug("Write packet end", zap.String("clientID", c.info.clientID), zap.Error(err))

	// TODO, disconnect on write error?
	if err != nil {
		c.Close()
	}
	return err
}
