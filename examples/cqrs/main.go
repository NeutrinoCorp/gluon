package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/neutrinocorp/gluon"
	"github.com/neutrinocorp/gluon/gcqrs"
	_ "github.com/neutrinocorp/gluon/gmemory"
)

type GetUserQuery struct {
	Username string `json:"username"`
}

func (q GetUserQuery) Key() string {
	return "org.neutrinocorp.cosmos.query.user.get"
}

type GetUserResponse struct {
	UserID       string    `json:"user_id"`
	Email        string    `json:"email"`
	Username     string    `json:"username"`
	Password     string    `json:"password"`
	LastEditTime time.Time `json:"last_edit_time"`
}

type GetUserQueryHandler struct {
}

var _ gcqrs.QueryHandler = GetUserQueryHandler{}

func (h GetUserQueryHandler) Handle(_ context.Context, q interface{}) (interface{}, error) {
	log.Printf("q: %v\n", q)
	return GetUserResponse{
		UserID:       "1",
		Email:        "aruiz@neutrinocorp.org",
		Username:     "aruiz",
		Password:     "12345678",
		LastEditTime: time.Now().UTC(),
	}, nil
}

type CreateUserCommand struct {
	UserID   string `json:"user_id"`
	Email    string `json:"email"`
	Username string `json:"username"`
	Password string `json:"password"`
}

func (c CreateUserCommand) Key() string {
	return "org.neutrinocorp.cosmos.command.user.create"
}

type CreateUserCommandHandler struct{}

func (c CreateUserCommandHandler) Handle(_ context.Context, cmd interface{}) error {
	log.Printf("cmd: %v\n", cmd)
	return nil
}

var _ gcqrs.CommandHandler = CreateUserCommandHandler{}

func main() {
	b := gluon.NewBroker("memory",
		gluon.WithSource("org.neutrinocorp/cosmos/users"))
	commandBus := gcqrs.NewCommandBus(b)
	queryBus := gcqrs.NewQueryBus()

	cmd := CreateUserCommand{}
	_ = commandBus.Register(cmd, CreateUserCommandHandler{})

	query := GetUserQuery{}
	_ = queryBus.Register(query, GetUserQueryHandler{})

	// graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	go func() {
		if err := b.ListenAndServe(); err != nil && err != gluon.ErrBrokerClosed {
			log.Fatal(err)
		}
	}()

	go func() {
		ctx := context.Background()
		query := GetUserQuery{
			Username: "aruiz",
		}
		res, err := queryBus.Ask(ctx, query)
		if err != nil {
			log.Print(err)
			return
		}

		resJSON, _ := json.Marshal(res)
		log.Printf("res: %v\n", string(resJSON))
	}()

	go func() {
		ctx := context.Background()
		cmd := CreateUserCommand{
			UserID:   "1",
			Email:    "aruiz@neutrinocorp.org",
			Username: "aruiz",
			Password: "12345678",
		}
		_, _ = commandBus.Dispatch(ctx, cmd)
	}()

	go func() {
		ctx := context.Background()
		cmd := CreateUserCommand{
			UserID:   "2",
			Email:    "br1@neutrinocorp.org",
			Username: "br1",
			Password: "987456123",
		}
		_, _ = commandBus.Dispatch(ctx, cmd)
	}()

	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	if err := b.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}
