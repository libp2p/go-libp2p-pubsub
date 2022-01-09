package pubsub

import "sync"

type msgIDGenerator struct {
	defGen MsgIdFunction

	topicGens    map[string]MsgIdFunction
	topicGensLk sync.RWMutex
}

func (m *msgIDGenerator) Add(topic string, gen MsgIdFunction) {
	m.topicGensLk.Lock()
	m.topicGens[topic] = gen
	m.topicGensLk.Unlock()
}

func (m *msgIDGenerator) GenID(msg *Message) string {
	if msg.ID != "" {
		return msg.ID
	}

	m.topicGensLk.RLock()
	gen, ok := m.topicGens[msg.GetTopic()]
	m.topicGensLk.RUnlock()
	if !ok {
		gen = m.defGen
	}

	msg.ID = gen(msg.Message)
	return msg.ID
}
