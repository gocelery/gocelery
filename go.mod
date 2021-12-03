module github.com/pnongah/gocelery

go 1.16

require (
	github.com/gocelery/gocelery v0.0.0-20201111034804-825d89059344
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/satori/go.uuid v1.2.1-0.20181028125025-b2ce2384e17b
	github.com/streadway/amqp v0.0.0-20190827072141-edfb9018d271
)

replace github.com/gocelery/gocelery => ./
