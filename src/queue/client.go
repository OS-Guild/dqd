package queue

type Message interface {
	Id() string
	Data() string
	Fail()
	Done()
}

type Client interface {
	Iter(messages chan Message, stop chan bool)
}

type ClientFactory interface {
	Create() Client
}
