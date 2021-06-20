package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/neutrinocorp/gluon"
	"github.com/neutrinocorp/gluon/gcqrs"
	_ "github.com/neutrinocorp/gluon/gmemory"
)

type CreateUserCommand struct {
	UserID   string `json:"user_id"`
	Email    string `json:"email"`
	Username string `json:"username"`
	Password string `json:"password"`
}

func (c CreateUserCommand) Key() string {
	return "org.neutrinocorp.cosmos.command.user.create"
}

var _ gcqrs.Command = CreateUserCommand{}

type CreateUserCommandHandler struct{}

func (c CreateUserCommandHandler) Command() gcqrs.Command {
	return CreateUserCommand{}
}

func (c CreateUserCommandHandler) Handle(ctx context.Context, cmd gcqrs.Command) error {
	log.Printf("cmd: %v\n", cmd)
	return nil
}

var _ gcqrs.CommandHandler = CreateUserCommandHandler{}

func main() {
	b := gluon.NewBroker("memory", "user-service")
	commandBus := gcqrs.NewCommandBus(b)

	cmd := CreateUserCommand{}
	commandBus.Register(cmd, CreateUserCommandHandler{})

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
		cmd := CreateUserCommand{
			UserID:   "1",
			Email:    "aruiz@neutrinocorp.org",
			Username: "aruiz",
			Password: "12345678",
		}
		commandBus.Exec(ctx, cmd)
	}()

	go func() {
		ctx := context.Background()
		cmd := CreateUserCommand{
			UserID:   "2",
			Email:    "br1@neutrinocorp.org",
			Username: "br1",
			Password: "987456123",
		}
		commandBus.Exec(ctx, cmd)
	}()

	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	if err := b.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}
