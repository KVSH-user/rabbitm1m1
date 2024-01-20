// main.go (в директории M1)
package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/streadway/amqp"
)

type Message struct {
	Number int `json:"number"`
}

type Response struct {
	Result int `json:"result"`
}

func main() {
	http.HandleFunc("/process", ProcessHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func ProcessHandler(w http.ResponseWriter, r *http.Request) {
	var message Message
	if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer ch.Close()

	resultsQueue, err := ch.QueueDeclare(
		"results",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resultsQueueName := resultsQueue.Name + r.Header.Get("X-Request-ID")

	body, err := json.Marshal(message)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = ch.Publish(
		"",
		"tasks",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			ReplyTo:     resultsQueueName,
			Body:        body,
		})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	msgs, err := ch.Consume(
		resultsQueueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var result int
	select {
	case msg := <-msgs:
		var resultMessage map[string]int
		if err := json.Unmarshal(msg.Body, &resultMessage); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result = resultMessage["result"]
		response := Response{
			Result: result,
		}
		responseJSON, err := json.Marshal(response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(responseJSON)
		return
	}
}
