package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pnongah/gocelery"

	"github.com/gomodule/redigo/redis"
	"github.com/keon94/go-compose/docker"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var Env = &docker.EnvironmentConfig{
	UpTimeout:   60 * time.Second,
	DownTimeout: 60 * time.Second,
	ComposeFilePaths: []string{
		"docker-compose.yml",
	},
}

func GetRabbitMQConnectionConfig(c *docker.Container) (interface{}, error) {
	host, ports, err := docker.GetAllEndpoints(c)
	if err != nil {
		return "", err
	}
	publicPorts := ports["5672"]
	if len(publicPorts) == 0 {
		return nil, errors.New("rabbitmq port not found")
	}
	var connString string
	for _, publicPort := range publicPorts {
		connString = fmt.Sprintf("amqp://admin:root@%s:%s", host, publicPort)
		conn, err := amqp.Dial(connString)
		if err == nil {
			_ = conn.Close()
			break
		}
		logrus.Infof("rabbitmq connection \"%s\" failed with %s. trying another if available.", connString, err.Error())
		connString = ""
	}
	if connString == "" {
		return nil, fmt.Errorf("no valid rabbitmq connection could be establised")
	}
	return connString, nil
}

func GetRedisConnectionConfig(c *docker.Container) (interface{}, error) {
	host, ports, err := docker.GetAllEndpoints(c)
	if err != nil {
		return "", err
	}
	publicPorts := ports["6379"]
	if len(publicPorts) == 0 {
		return nil, errors.New("redis port not found")
	}
	var connString string
	for _, publicPort := range publicPorts {
		connString = fmt.Sprintf("redis://%s:%s", host, publicPort)
		conn, err := redis.DialURL(connString)
		if err == nil {
			_ = conn.Close()
			break
		}
		logrus.Infof("redis connection \"%s\" failed with %s. trying another if available.", connString, err.Error())
		connString = ""
	}
	if connString == "" {
		return nil, fmt.Errorf("no valid redis connection could be establised")
	}
	return connString, nil
}

func GetCeleryClient(brokerUrl string, backendUrl string) (*gocelery.CeleryClient, error) {
	backend, err := getCeleryBackend(backendUrl)
	if err != nil {
		return nil, err
	}
	broker, err := getCeleryBroker(brokerUrl)
	if err != nil {
		return nil, err
	}
	return gocelery.NewCeleryClient(broker, backend, 2)
}

//============================== helpers ==========================================

func getCeleryBroker(url string) (gocelery.CeleryBroker, error) {
	if strings.HasPrefix(url, "redis://") {
		redisPool := &redis.Pool{
			Dial: func() (redis.Conn, error) {
				c, err := redis.DialURL(url)
				if err != nil {
					return nil, err
				}
				return c, err
			},
		}
		return &gocelery.RedisCeleryBroker{Pool: redisPool, QueueName: "celery"}, nil
	} else if strings.HasPrefix(url, "amqp://") {
		return gocelery.NewAMQPCeleryBroker(url, &amqp.Config{
			Heartbeat: 60 * time.Second,
		}, "celery")
	} else {
		return nil, fmt.Errorf("bad url scheme")
	}
}

func getCeleryBackend(url string) (gocelery.CeleryBackend, error) {
	if strings.HasPrefix(url, "redis://") {
		redisPool := &redis.Pool{
			Dial: func() (redis.Conn, error) {
				c, err := redis.DialURL(url)
				if err != nil {
					return nil, err
				}
				return c, err
			},
		}
		return &gocelery.RedisCeleryBackend{Pool: redisPool}, nil
	} else if strings.HasPrefix(url, "amqp://") {
		connection, err := amqp.DialConfig(url, amqp.Config{
			Heartbeat: 60 * time.Second,
		})
		if err != nil {
			return nil, err
		}
		channel, err := connection.Channel()
		if err != nil {
			return nil, err
		}
		return gocelery.NewAMQPCeleryBackendByConnAndChannel(connection, channel), nil
	} else {
		return nil, fmt.Errorf("bad url scheme")
	}
}
