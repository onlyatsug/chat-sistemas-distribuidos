defmodule ChatDistribuido.Metrics do
  @moduledoc """
  Pessoa 04: Coleta isolada de telemetria da VM Erlang/BEAM, catálogo ETS, presença CRDT
  e tráfego real de mensagens (via :telemetry do Phoenix) em todos os nós do cluster.
  """
  use GenServer

  alias ChatDistribuido.Cluster.Discovery
  alias ChatDistribuido.NodeInfo
  alias ChatDistribuidoWeb.Presence

  @dist_rooms :chat_distribuido_dist_rooms
  @local_rooms :chat_distribuido_rooms
  @counters_table :chat_distribuido_p4_counters
  @telemetry_handler_id "chat-distribuido-p4-channel-metrics"
  @rpc_timeout_ms 1_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    if not ets_exists?(@counters_table) do
      :ets.new(@counters_table, [
        :named_table,
        :public,
        :set,
        read_concurrency: true,
        write_concurrency: true
      ])
    end

    :ets.insert_new(@counters_table, {:mensagens, 0})
    :ets.insert_new(@counters_table, {:entregas, 0})
    :ets.insert_new(@counters_table, {:duracao_us, 0})

    :telemetry.detach(@telemetry_handler_id)

    :telemetry.attach(
      @telemetry_handler_id,
      [:phoenix, :channel_handled_in],
      &__MODULE__.handle_channel_event/4,
      nil
    )

    {:ok, %{}}
  end

  @doc false
  def handle_channel_event(
        [:phoenix, :channel_handled_in],
        %{duration: duration},
        %{event: "new_msg", socket: %{topic: "room:" <> _ = topic}},
        _config
      ) do
    if ets_exists?(@counters_table) do
      duration_us = System.convert_time_unit(duration, :native, :microsecond)
      deliveries = max(1, count_room_sessions(topic))

      :ets.update_counter(@counters_table, :mensagens, {2, 1}, {:mensagens, 0})
      :ets.update_counter(@counters_table, :entregas, {2, deliveries}, {:entregas, 0})
      :ets.update_counter(@counters_table, :duracao_us, {2, duration_us}, {:duracao_us, 0})
    end

    :ok
  rescue
    _ -> :ok
  end

  def handle_channel_event(_event, _measurements, _metadata, _config), do: :ok

  @doc "Retorna o panorama consolidado do cluster e as métricas individuais de cada nó BEAM."
  def cluster_metrics do
    local = local_snapshot()
    remote_nodes = Discovery.connected_nodes()
    {remote_snapshots, unreachable} = fetch_remote_snapshots(remote_nodes)
    presence = presence_summary()

    nodes =
      [local | remote_snapshots]
      |> Enum.map(fn snap ->
        Map.put(snap, :sessoes_crdt, Map.get(presence.por_no, snap.no, 0))
      end)
      |> Enum.sort_by(& &1.no)

    %{
      coletado_em:
        DateTime.utc_now() |> DateTime.truncate(:millisecond) |> DateTime.to_iso8601(),
      no_respondedor: local.no,
      modo: if(local.vivo, do: "cluster", else: "local"),
      resumo_cluster: aggregate_cluster(nodes, unreachable, presence),
      presenca_crdt: presence,
      nos: nodes
    }
  end

  @doc "Coleta métricas locais da VM Erlang/BEAM, tabelas ETS e contadores de mensagens deste nó."
  def local_snapshot do
    mem = :erlang.memory()
    {uptime_ms, _} = :erlang.statistics(:wall_clock)
    {reductions, _} = :erlang.statistics(:reductions)
    {{:input, io_in}, {:output, io_out}} = :erlang.statistics(:io)
    rooms = read_rooms()
    public_count = Enum.count(rooms, &(&1.tipo == "publica"))
    private_count = length(rooms) - public_count

    msgs_pub = read_counter(:mensagens)
    msgs_deliv = read_counter(:entregas)
    dur_us = read_counter(:duracao_us)

    %{
      no: NodeInfo.nome(),
      vivo: Node.alive?(),
      uptime_segundos: div(uptime_ms, 1_000),
      processos: %{
        ativos: :erlang.system_info(:process_count),
        limite: :erlang.system_info(:process_limit)
      },
      portas_io: %{
        ativas: :erlang.system_info(:port_count),
        sockets_otp: otp_socket_count()
      },
      schedulers: %{
        online: :erlang.system_info(:schedulers_online),
        total: :erlang.system_info(:schedulers),
        run_queue: :erlang.statistics(:total_run_queue_lengths_all)
      },
      memoria_mb: %{
        total: bytes_to_mb(mem[:total]),
        processos: bytes_to_mb(mem[:processes]),
        ets: bytes_to_mb(mem[:ets]),
        binarios: bytes_to_mb(mem[:binary]),
        sistema: bytes_to_mb(mem[:system])
      },
      atividade_vm: %{
        reducoes: reductions,
        io_entrada_kb: Float.round(io_in / 1_024, 1),
        io_saida_kb: Float.round(io_out / 1_024, 1)
      },
      mensagens_no: %{
        publicadas: msgs_pub,
        entregas_estimadas: msgs_deliv,
        duracao_total_us: dur_us,
        latencia_servidor_ms: avg_ms(dur_us, msgs_pub)
      },
      catalogo_local: %{
        salas_total: length(rooms),
        salas_publicas: public_count,
        salas_privadas: private_count
      }
    }
  end

  defp fetch_remote_snapshots([]), do: {[], 0}

  defp fetch_remote_snapshots(remote_nodes) do
    results =
      try do
        :erpc.multicall(remote_nodes, __MODULE__, :local_snapshot, [], @rpc_timeout_ms)
      rescue
        _ -> Enum.map(remote_nodes, fn _ -> {:error, :rpc_failed} end)
      end

    Enum.reduce(results, {[], 0}, fn
      {:ok, snapshot}, {acc, failed} when is_map(snapshot) -> {[snapshot | acc], failed}
      _, {acc, failed} -> {acc, failed + 1}
    end)
  end

  defp aggregate_cluster(nodes, unreachable, presence) do
    total_mem =
      nodes
      |> Enum.reduce(0.0, fn n, acc -> acc + n.memoria_mb.total end)
      |> Float.round(2)

    total_msgs = Enum.reduce(nodes, 0, fn n, acc -> acc + n.mensagens_no.publicadas end)
    total_deliv = Enum.reduce(nodes, 0, fn n, acc -> acc + n.mensagens_no.entregas_estimadas end)
    total_dur_us = Enum.reduce(nodes, 0, fn n, acc -> acc + n.mensagens_no.duracao_total_us end)

    %{
      total_nos: length(nodes),
      nos_inacessiveis: unreachable,
      sessoes_ativas: presence.sessoes_ativas,
      usuarios_unicos: presence.usuarios_unicos,
      processos_cluster: Enum.reduce(nodes, 0, fn n, acc -> acc + n.processos.ativos end),
      memoria_cluster_mb: total_mem,
      schedulers_cluster: Enum.reduce(nodes, 0, fn n, acc -> acc + n.schedulers.online end),
      run_queue_cluster: Enum.reduce(nodes, 0, fn n, acc -> acc + n.schedulers.run_queue end),
      mensagens_cluster: total_msgs,
      entregas_cluster: total_deliv,
      latencia_servidor_ms: avg_ms(total_dur_us, total_msgs),
      salas_replicadas:
        nodes |> Enum.map(& &1.catalogo_local.salas_total) |> Enum.max(fn -> 0 end)
    }
  end

  defp presence_summary do
    rooms = read_rooms() |> Enum.take(100)

    {por_sala, por_no, total_sessoes, usuarios_unicos} =
      Enum.reduce(rooms, {[], %{}, 0, MapSet.new()}, fn room,
                                                        {salas_acc, nos_acc, sess_acc, users_acc} ->
        entries = safe_presence_list("room:" <> room.id)

        {room_sessions, nos_acc, users_acc} =
          Enum.reduce(entries, {0, nos_acc, users_acc}, fn {user, %{metas: metas}},
                                                           {r_sess, n_acc, u_acc} ->
            n_acc =
              Enum.reduce(metas, n_acc, fn meta, acc ->
                node_name = Map.get(meta, :no) || Map.get(meta, "no") || "desconhecido"
                Map.update(acc, node_name, 1, &(&1 + 1))
              end)

            {r_sess + length(metas), n_acc, MapSet.put(u_acc, user)}
          end)

        salas_acc =
          if room_sessions > 0 or room.id == "geral" do
            [%{id: room.id, nome: room.nome, tipo: room.tipo, online: room_sessions} | salas_acc]
          else
            salas_acc
          end

        {salas_acc, nos_acc, sess_acc + room_sessions, users_acc}
      end)

    %{
      sessoes_ativas: total_sessoes,
      usuarios_unicos: MapSet.size(usuarios_unicos),
      por_no: por_no,
      salas_ativas: Enum.sort_by(por_sala, &{-&1.online, &1.nome})
    }
  end

  defp count_room_sessions(topic) do
    topic
    |> safe_presence_list()
    |> Enum.reduce(0, fn {_user, %{metas: metas}}, acc -> acc + length(metas) end)
  end

  defp read_counter(key) do
    if ets_exists?(@counters_table) do
      case :ets.lookup(@counters_table, key) do
        [{^key, val}] -> val
        _ -> 0
      end
    else
      0
    end
  end

  defp avg_ms(_dur_us, 0), do: 0.0
  defp avg_ms(dur_us, count), do: Float.round(dur_us / count / 1_000, 2)

  defp read_rooms do
    cond do
      ets_exists?(@dist_rooms) ->
        :ets.tab2list(@dist_rooms) |> Enum.map(fn {_, room} -> room end)

      ets_exists?(@local_rooms) ->
        :ets.tab2list(@local_rooms) |> Enum.map(fn {_, room} -> room end)

      true ->
        []
    end
  end

  defp ets_exists?(table), do: :ets.whereis(table) != :undefined

  defp safe_presence_list(topic) do
    if Process.whereis(Presence) do
      Presence.list(topic)
    else
      %{}
    end
  rescue
    _ -> %{}
  end

  defp otp_socket_count do
    :socket.number_of()
  rescue
    _ -> 0
  catch
    _, _ -> 0
  end

  defp bytes_to_mb(bytes) when is_integer(bytes), do: Float.round(bytes / 1_048_576, 2)
  defp bytes_to_mb(_), do: 0.0
end
