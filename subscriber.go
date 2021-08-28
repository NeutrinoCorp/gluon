package gluon

type Subscriber struct {
	key string

	group       string
	handler     Handler
	handlerFunc HandlerFunc
}

func newSubscriber(key string) *Subscriber {
	return &Subscriber{
		key: key,
	}
}

func (e *Subscriber) Group(g string) *Subscriber {
	e.group = g
	return e
}

func (e *Subscriber) Handler(h Handler) *Subscriber {
	e.handler = h
	return e
}

func (e *Subscriber) HandlerFunc(h HandlerFunc) *Subscriber {
	e.handlerFunc = h
	return e
}

func (e Subscriber) getHandler() HandlerFunc {
	if e.handlerFunc != nil {
		return e.handlerFunc
	}
	return e.handler.Handle
}
