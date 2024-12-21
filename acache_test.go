package pubsub

import (
	"github.com/libp2p/go-libp2p/core/peer"

	"testing"
	"time"
)

type Event struct {
	// True if it's a send event, false if it's a timeout event
	send bool
	meta *IneedMeta
	time time.Time
}

func closeTimes(a time.Time, b time.Time) bool {
	return a.Sub(b) < 2*time.Millisecond && b.Sub(a) < 2*time.Millisecond
}

func TestAnnounceCache(t *testing.T) {
	var events []Event
	timeout := 50 * time.Millisecond
	ac := NewAnnounceCache(timeout)
	pidA := peer.ID("A")
	pidB := peer.ID("B")
	pidC := peer.ID("C")
	pidD := peer.ID("D")

	done := make(chan struct{})
	go func() {
		timer := time.After(200 * time.Millisecond)
		for {
			select {
			case meta := <-ac.T:
				events = append(events, Event{
					send: false,
					meta: meta,
					time: time.Now(),
				})
			case meta := <-ac.R:
				events = append(events, Event{
					send: true,
					meta: meta,
					time: time.Now(),
				})
			case <-timer:
				done <- struct{}{}
				return
			}
		}
	}()

	start := time.Now()
	ac.Add("mid1", pidA)
	ac.Add("mid1", pidB)
	ac.Add("mid2", pidC)
	ac.Add("mid2", pidD)

	<-done

	// Check the number of events
	if len(events) != 8 {
		t.Fatal("incorrect number of events")
	}
	msgList := map[string][]peer.ID{
		"mid1": {pidA, pidB},
		"mid2": {pidC, pidD},
	}
	sentTime := make(map[string]time.Time)
	expiryTime := make(map[string]time.Time)
	for _, event := range events {
		if event.send {
			if _, ok := sentTime[event.meta.mid]; ok {
				t.Fatal("there shouldn't be an ongoing INEED when send event is received")
			}
			if msgList[event.meta.mid][0] != event.meta.pid {
				t.Fatal("wrong peer id in the send event")
			}
			expiryT, ok := expiryTime[event.meta.mid]
			var expectedSentTime time.Time
			if ok {
				expectedSentTime = expiryT
			} else {
				expectedSentTime = start
			}
			if !closeTimes(expectedSentTime, event.time) {
				t.Fatal("send event is not sent timely")
			}
			sentTime[event.meta.mid] = event.time
		} else {
			sentT, ok := sentTime[event.meta.mid]
			if !ok {
				t.Fatal("there shouldn be an ongoing INEED when timeout event is received")
			}
			if msgList[event.meta.mid][0] != event.meta.pid {
				t.Fatal("wrong peer id in the send event")
			}
			if !closeTimes(sentT.Add(timeout), event.time) {
				t.Fatal("timeout event is not sent timely")
			}
			delete(sentTime, event.meta.mid)
			expiryTime[event.meta.mid] = event.time
			msgList[event.meta.mid] = msgList[event.meta.mid][1:]
		}
	}
	// Make sure that all send events have corresponding timeout events
	if len(sentTime) != 0 {
		t.Fatal("there is some send event that doesn't have corresponding timeout event")
	}
	// Make sure that all peer ids are consumed
	for mid, list := range msgList {
		if len(list) != 0 {
			t.Fatalf("%s has some peer id that doesn't have events", mid)
		}
	}
}

func TestAnnounceCacheClear(t *testing.T) {
	var events []Event
	timeout := 50 * time.Millisecond
	ac := NewAnnounceCache(timeout)
	pidA := peer.ID("A")
	pidB := peer.ID("B")
	pidC := peer.ID("C")
	pidD := peer.ID("D")

	done := make(chan struct{})
	go func() {
		timer := time.After(200 * time.Millisecond)
		for {
			select {
			case meta := <-ac.T:
				events = append(events, Event{
					send: false,
					meta: meta,
					time: time.Now(),
				})
			case meta := <-ac.R:
				events = append(events, Event{
					send: true,
					meta: meta,
					time: time.Now(),
				})
			case <-timer:
				done <- struct{}{}
				return
			}
		}
	}()

	go func() {
		time.Sleep(80 * time.Millisecond)
		ac.Clear("mid1")
	}()

	start := time.Now()
	ac.Add("mid1", pidA)
	ac.Add("mid1", pidB)
	ac.Add("mid1", pidC)
	ac.Add("mid2", pidD)

	<-done

	// Check the number of events
	if len(events) != 5 {
		t.Fatal("incorrect number of events")
	}
	msgList := map[string][]peer.ID{
		"mid1": {pidA, pidB},
		"mid2": {pidD},
	}
	sentTime := make(map[string]time.Time)
	expiryTime := make(map[string]time.Time)
	for _, event := range events {
		if event.send {
			if _, ok := sentTime[event.meta.mid]; ok {
				t.Fatal("there shouldn't be an ongoing INEED when send event is received")
			}
			if msgList[event.meta.mid][0] != event.meta.pid {
				t.Fatal("wrong peer id in the send event")
			}
			expiryT, ok := expiryTime[event.meta.mid]
			var expectedSentTime time.Time
			if ok {
				expectedSentTime = expiryT
			} else {
				expectedSentTime = start
			}
			if !closeTimes(expectedSentTime, event.time) {
				t.Fatal("send event is not sent timely")
			}
			sentTime[event.meta.mid] = event.time
		} else {
			sentT, ok := sentTime[event.meta.mid]
			if !ok {
				t.Fatal("there shouldn be an ongoing INEED when timeout event is received")
			}
			if msgList[event.meta.mid][0] != event.meta.pid {
				t.Fatal("wrong peer id in the send event")
			}
			if !closeTimes(sentT.Add(timeout), event.time) {
				t.Fatal("timeout event is not sent timely")
			}
			delete(sentTime, event.meta.mid)
			expiryTime[event.meta.mid] = event.time
			msgList[event.meta.mid] = msgList[event.meta.mid][1:]
		}
	}
	// Make sure that there is only one send event that doesn't have corresponding timeout event
	if len(sentTime) != 1 {
		t.Fatal("there should only be one send event that doesn't have corresponding timeout event")
	}
	if _, ok := sentTime["mid1"]; !ok {
		t.Fatal("One send event that doesn't have corresponding timeout event should be mid1")
	}
	// Make sure that all peer ids are consumed
	for mid, list := range msgList {
		if mid == "mid1" {
			if len(list) != 1 || list[0] != pidB {
				t.Fatal("there should be only pidB that isn't consumed and has no timeout event")
			}
		} else {
			if len(list) != 0 {
				t.Fatalf("%s has some peer id that doesn't have events", mid)
			}
		}
	}
}

func TestAnnounceCacheReAdd(t *testing.T) {
	var events []Event
	timeout := 50 * time.Millisecond
	ac := NewAnnounceCache(timeout)
	pidA := peer.ID("A")
	pidB := peer.ID("B")
	pidC := peer.ID("C")
	pidD := peer.ID("D")

	done := make(chan struct{})
	go func() {
		timer := time.After(300 * time.Millisecond)
		for {
			select {
			case meta := <-ac.T:
				events = append(events, Event{
					send: false,
					meta: meta,
					time: time.Now(),
				})
			case meta := <-ac.R:
				events = append(events, Event{
					send: true,
					meta: meta,
					time: time.Now(),
				})
			case <-timer:
				done <- struct{}{}
				return
			}
		}
	}()

	start := time.Now()
	ac.Add("mid1", pidA)
	ac.Add("mid1", pidB)
	ac.Add("mid2", pidC)
	time.Sleep(220 * time.Millisecond)
	start2 := time.Now()
	ac.Add("mid1", pidD)

	<-done

	// Check the number of events
	if len(events) != 8 {
		t.Fatal("incorrect number of events")
	}
	msgList := map[string][]peer.ID{
		"mid1": {pidA, pidB, pidD},
		"mid2": {pidC},
	}
	sentTime := make(map[string]time.Time)
	expiryTime := make(map[string]time.Time)
	for _, event := range events {
		if event.send {
			if _, ok := sentTime[event.meta.mid]; ok {
				t.Fatal("there shouldn't be an ongoing INEED when send event is received")
			}
			if msgList[event.meta.mid][0] != event.meta.pid {
				t.Fatal("wrong peer id in the send event")
			}
			expiryT, ok := expiryTime[event.meta.mid]
			var expectedSentTime time.Time
			if event.meta.pid == pidD {
				expectedSentTime = start2
			} else if ok {
				expectedSentTime = expiryT
			} else {
				expectedSentTime = start
			}
			if !closeTimes(expectedSentTime, event.time) {
				t.Fatal("send event is not sent timely")
			}
			sentTime[event.meta.mid] = event.time
		} else {
			sentT, ok := sentTime[event.meta.mid]
			if !ok {
				t.Fatal("there shouldn be an ongoing INEED when timeout event is received")
			}
			if msgList[event.meta.mid][0] != event.meta.pid {
				t.Fatal("wrong peer id in the send event")
			}
			if !closeTimes(sentT.Add(timeout), event.time) {
				t.Fatal("timeout event is not sent timely")
			}
			delete(sentTime, event.meta.mid)
			expiryTime[event.meta.mid] = event.time
			msgList[event.meta.mid] = msgList[event.meta.mid][1:]
		}
	}
	// Make sure that all send events have corresponding timeout events
	if len(sentTime) != 0 {
		t.Fatal("there is some send event that doesn't have corresponding timeout event")
	}
	// Make sure that all peer ids are consumed
	for mid, list := range msgList {
		if len(list) != 0 {
			t.Fatalf("%s has some peer id that doesn't have events", mid)
		}
	}
}

func TestAnnounceCacheStop(t *testing.T) {
	var events []Event
	timeout := 50 * time.Millisecond
	ac := NewAnnounceCache(timeout)
	pidA := peer.ID("A")
	pidB := peer.ID("B")
	pidC := peer.ID("C")
	pidD := peer.ID("D")

	done := make(chan struct{})
	go func() {
		timer := time.After(200 * time.Millisecond)
		for {
			select {
			case meta := <-ac.T:
				events = append(events, Event{
					send: false,
					meta: meta,
					time: time.Now(),
				})
			case meta := <-ac.R:
				events = append(events, Event{
					send: true,
					meta: meta,
					time: time.Now(),
				})
			case <-timer:
				done <- struct{}{}
				return
			}
		}
	}()

	go func() {
		time.Sleep(80 * time.Millisecond)
		ac.Stop()
	}()

	start := time.Now()
	ac.Add("mid1", pidA)
	ac.Add("mid1", pidB)
	ac.Add("mid1", pidC)
	ac.Add("mid2", pidD)

	<-done

	// Check the number of events
	if len(events) != 5 {
		t.Fatal("incorrect number of events")
	}
	msgList := map[string][]peer.ID{
		"mid1": {pidA, pidB},
		"mid2": {pidD},
	}
	sentTime := make(map[string]time.Time)
	expiryTime := make(map[string]time.Time)
	for _, event := range events {
		if event.send {
			if _, ok := sentTime[event.meta.mid]; ok {
				t.Fatal("there shouldn't be an ongoing INEED when send event is received")
			}
			if msgList[event.meta.mid][0] != event.meta.pid {
				t.Fatal("wrong peer id in the send event")
			}
			expiryT, ok := expiryTime[event.meta.mid]
			var expectedSentTime time.Time
			if ok {
				expectedSentTime = expiryT
			} else {
				expectedSentTime = start
			}
			if !closeTimes(expectedSentTime, event.time) {
				t.Fatal("send event is not sent timely")
			}
			sentTime[event.meta.mid] = event.time
		} else {
			sentT, ok := sentTime[event.meta.mid]
			if !ok {
				t.Fatal("there shouldn be an ongoing INEED when timeout event is received")
			}
			if msgList[event.meta.mid][0] != event.meta.pid {
				t.Fatal("wrong peer id in the send event")
			}
			if !closeTimes(sentT.Add(timeout), event.time) {
				t.Fatal("timeout event is not sent timely")
			}
			delete(sentTime, event.meta.mid)
			expiryTime[event.meta.mid] = event.time
			msgList[event.meta.mid] = msgList[event.meta.mid][1:]
		}
	}
	// Make sure that there is only one send event that doesn't have corresponding timeout event
	if len(sentTime) != 1 {
		t.Fatal("there should only be one send event that doesn't have corresponding timeout event")
	}
	if _, ok := sentTime["mid1"]; !ok {
		t.Fatal("One send event that doesn't have corresponding timeout event should be mid1")
	}
	// Make sure that all peer ids are consumed
	for mid, list := range msgList {
		if mid == "mid1" {
			if len(list) != 1 || list[0] != pidB {
				t.Fatal("there should be only pidB that isn't consumed and has no timeout event")
			}
		} else {
			if len(list) != 0 {
				t.Fatalf("%s has some peer id that doesn't have events", mid)
			}
		}
	}
}
