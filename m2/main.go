// main.go (в директории M2)
package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/streadway/amqp"
)

type Message struct {
	Number int `json:"number"`
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal("Ошибка при подключении к RabbitMQ:", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("Ошибка при создании канала RabbitMQ:", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		"tasks",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("Ошибка при объявлении очереди заданий:", err)
	}

	resultsQueue, err := ch.QueueDeclare(
		"results",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("Ошибка при объявлении очереди результатов:", err)
	}

	msgs, err := ch.Consume(
		"tasks",
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("Ошибка при подписке на очередь заданий:", err)
	}

	for msg := range msgs {
		var task Message
		if err := json.Unmarshal(msg.Body, &task); err != nil {
			log.Println("Ошибка при разборе задания:", err)
			continue
		}

		time.Sleep(5 * time.Second)

		result := task.Number * 2
		log.Printf("M2: Задание %d обработано, результат: %d\n", task.Number, result)

		resultBody, err := json.Marshal(map[string]int{"result": result})
		if err != nil {
			log.Println("Ошибка при маршалинге результата:", err)
			continue
		}
		err = ch.Publish(
			"",
			resultsQueue.Name,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        resultBody,
			})
		if err != nil {
			log.Println("Ошибка при отправке результата в очередь:", err)
			continue
		}
	}
}
