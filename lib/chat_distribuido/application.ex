defmodule ChatDistribuido.Application do
  @moduledoc "Inicia os serviços da aplicação, incluindo PubSub distribuído, Presença CRDT, Catálogo e Cluster."
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Phoenix.PubSub, name: ChatDistribuido.PubSub},
      ChatDistribuidoWeb.Presence,
      ChatDistribuido.Rooms.Store.child_spec(),
      ChatDistribuido.Cluster.Discovery,
      ChatDistribuido.Metrics,
      ChatDistribuidoWeb.Endpoint
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: ChatDistribuido.Supervisor)
  end

  @impl true
  def config_change(changed, _new, removed) do
    ChatDistribuidoWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
