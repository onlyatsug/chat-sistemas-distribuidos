defmodule ChatDistribuido.MetricsTest do
  use ExUnit.Case, async: false
  import Plug.Test
  import Phoenix.ChannelTest

  @endpoint ChatDistribuidoWeb.Endpoint

  test "endpoint /metrics retorna telemetria da BEAM, catálogo ETS e presença CRDT" do
    {:ok, _, socket} =
      socket(ChatDistribuidoWeb.UserSocket, "metricas-ana", %{})
      |> subscribe_and_join(ChatDistribuidoWeb.RoomChannel, "room:geral", %{"usuario" => "Ana"})

    # Aguarda o :after_join registrar a presença CRDT e envia uma mensagem real
    assert_push("presence_state", _)
    ref = push(socket, "new_msg", %{"texto" => "Mensagem real para telemetria"})
    assert_reply(ref, :ok, %{id: _})

    conn = ChatDistribuidoWeb.Router.call(conn(:get, "/metrics"), [])
    assert conn.status == 200

    body = Jason.decode!(conn.resp_body)

    assert is_binary(body["coletado_em"])
    assert is_binary(body["no_respondedor"])
    assert body["resumo_cluster"]["total_nos"] >= 1
    assert body["resumo_cluster"]["processos_cluster"] > 0
    assert body["resumo_cluster"]["memoria_cluster_mb"] > 0
    assert body["resumo_cluster"]["mensagens_cluster"] >= 1
    assert body["resumo_cluster"]["entregas_cluster"] >= 1
    assert body["presenca_crdt"]["sessoes_ativas"] >= 1

    [node | _] = body["nos"]
    assert is_integer(node["processos"]["ativos"])
    assert is_number(node["memoria_mb"]["total"])
    assert is_number(node["memoria_mb"]["ets"])
    assert is_integer(node["schedulers"]["online"])
    assert node["catalogo_local"]["salas_total"] >= 1
  end
end
