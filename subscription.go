package floodsub

type Subscription struct {
	topic    string
	ch       chan *Message
	cancelCh chan<- *Subscription
	err      error
}

func (sub *Subscription) Topic() string {
	return sub.topic
}

func (sub *Subscription) Next() (*Message, error) {
	msg, ok := <-sub.ch

	if !ok {
		return msg, sub.err
	}

	return msg, nil
}

func (sub *Subscription) Cancel() {
	sub.cancelCh <- sub
}
