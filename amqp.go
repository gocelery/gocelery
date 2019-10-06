// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"errors"
	"log"

	"github.com/streadway/amqp"
)

const (
	defaultQueueName = "celery"
	JSONContentType  = "application/json"
)

var (
	// ErrConnection is returned when connection is dropped
	ErrConnection = errors.New("amqp connection error")
	// ErrBlocked is returned when connection is blocked
	ErrBlocked = errors.New("amqp channel blocked")
	// ErrChannelEmpty is returned when consuming channel is empty
	ErrChannelEmpty = errors.New("consuming channel empty")
)

type AMQPConnection struct {
	ConnStr   string // connection string
	QueueName string
	Conn      *amqp.Connection
	Channel   *amqp.Channel
	CloseErr  chan *amqp.Error
	BlockErr  chan amqp.Blocking
}

func (c *AMQPConnection) createConn() (err error) {
	c.Conn, err = amqp.Dial(c.ConnStr)
	if err != nil {
		return
	}
	c.Conn.NotifyClose(c.CloseErr)
	c.Conn.NotifyBlocked(c.BlockErr)
	return nil
}

func (c *AMQPConnection) closeConn() error {
	return c.Conn.Close()
}

func (c *AMQPConnection) createChannel() (err error) {
	c.Channel, err = c.Conn.Channel()
	if err != nil {
		return
	}
	c.Channel.NotifyClose(c.CloseErr)
	return nil
}

func (c *AMQPConnection) closeChannel() error {
	return c.Channel.Close()
}

func (c *AMQPConnection) getQueueName() string {
	if c.QueueName == "" {
		return defaultQueueName
	}
	return c.QueueName
}

// Connect ensures connection and channel exist
func (c *AMQPConnection) Connect() error {

	// initialize err channels
	if c.CloseErr == nil {
		c.CloseErr = make(chan *amqp.Error, 1)
	}
	if c.BlockErr == nil {
		c.BlockErr = make(chan amqp.Blocking, 1)
	}

	// establish connection
	if c.Conn == nil {
		if err := c.createConn(); err != nil {
			return err
		}
	}

	// create channel
	if c.Channel == nil {
		if err := c.createChannel(); err != nil {
			return err
		}
	}

	// check error channels
	select {
	case <-c.CloseErr:
		return ErrConnection
	case <-c.BlockErr:
		return ErrBlocked
	default:
		return nil
	}
}

// Close closes channel and connection
func (c *AMQPConnection) Close() error {
	c.Channel.Close()
	return c.Conn.Close()
}

// deliveryAck acknowledges delivery message with retries on error
func deliveryAck(delivery amqp.Delivery) {
	var err error
	for retryCount := 3; retryCount > 0; retryCount-- {
		if err = delivery.Ack(false); err == nil {
			break
		}
	}
	if err != nil {
		log.Printf("amqp_backend: failed to acknowledge result message %+v: %+v", delivery.MessageId, err)
	}
}
