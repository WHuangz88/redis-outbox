package consul

import (
	"context"
	"fmt"

	consulapi "github.com/hashicorp/consul/api"
)

// ConsulRegistry implements ports.ServiceRegistry using the official Consul Client API.
type ConsulRegistry struct {
	client *consulapi.Client
}

// NewConsulRegistry initializes the Consul Client.
func NewConsulRegistry(addr string) (*ConsulRegistry, error) {
	cfg := consulapi.DefaultConfig()
	cfg.Address = addr
	client, err := consulapi.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &ConsulRegistry{client: client}, nil
}

// Register registers this service instance with a dynamic Consul HTTP health check.
func (r *ConsulRegistry) Register(ctx context.Context, id, name, host string, port int) error {
	registration := &consulapi.AgentServiceRegistration{
		ID:      id,
		Name:    name,
		Address: host,
		Port:    port,
		Check: &consulapi.AgentServiceCheck{
			HTTP:     fmt.Sprintf("http://%s:%d/health", host, port),
			Interval: "10s",
			Timeout:  "5s",
		},
	}
	return r.client.Agent().ServiceRegister(registration)
}

// Deregister removes the service registration mapping from Consul agent.
func (r *ConsulRegistry) Deregister(ctx context.Context, id string) error {
	return r.client.Agent().ServiceDeregister(id)
}

// NoOpRegistry (Null Object Pattern) implements ports.ServiceRegistry when Consul is disabled.
type NoOpRegistry struct{}

// NewNoOpRegistry instantiates the safe no-op registry.
func NewNoOpRegistry() *NoOpRegistry {
	return &NoOpRegistry{}
}

func (r *NoOpRegistry) Register(ctx context.Context, id, name, host string, port int) error {
	return nil
}

func (r *NoOpRegistry) Deregister(ctx context.Context, id string) error {
	return nil
}
