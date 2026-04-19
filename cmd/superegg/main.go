package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/crypto/bcrypt"

	"superegg/internal/app"
	"superegg/internal/config"
)

func main() {
	log.SetFlags(0)

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "hash-password":
			if err := runHashPassword(os.Args[2:]); err != nil {
				log.Fatal(err)
			}
			return
		case "refresh":
			if err := runRefresh(os.Args[2:]); err != nil {
				log.Fatal(err)
			}
			return
		}
	}

	if err := runServe(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	configPath := fs.String("config", "config.yaml", "path to config file")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	instance, err := app.New(ctx, cfg)
	if err != nil {
		return err
	}
	defer instance.Close()

	log.Printf("superegg listening on http://%s", cfg.Server.ListenAddr)
	return instance.Run(ctx)
}

func runRefresh(args []string) error {
	fs := flag.NewFlagSet("refresh", flag.ContinueOnError)
	configPath := fs.String("config", "config.yaml", "path to config file")
	timeout := fs.Duration("timeout", 2*time.Minute, "how long to wait for the queue to drain")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	instance, err := app.New(ctx, cfg)
	if err != nil {
		return err
	}
	defer instance.Close()

	if err := instance.EnqueueAllRefreshes(ctx); err != nil {
		return err
	}
	if err := instance.DrainQueue(ctx); err != nil {
		return err
	}
	log.Printf("refresh completed")
	return nil
}

func runHashPassword(args []string) error {
	fs := flag.NewFlagSet("hash-password", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: superegg hash-password \"your-password\"")
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(fs.Arg(0)), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}
	fmt.Println(string(hash))
	return nil
}
