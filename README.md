## Custom small Rabbitmq go library

Use it as customRabbitmq

Example:

`rabbitmq := customRabbitmq.NewRabbitmqClient()`
`rabbitmq.CreateQueues([]string{"hello"})`