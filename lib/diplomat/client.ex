defmodule Diplomat.Client do
  alias Diplomat.{Entity, QueryResultBatch, Key}

  alias Diplomat.Proto.{
    AllocateIdsRequest,
    AllocateIdsResponse,
    CommitRequest,
    CommitResponse,
    BeginTransactionRequest,
    BeginTransactionResponse,
    RollbackRequest,
    RollbackResponse,
    RunQueryRequest,
    RunQueryResponse,
    LookupRequest,
    LookupResponse,
    Status
  }

  @moduledoc """
  Low level Google DataStore RPC client functions.
  """

  @api_version "v1"

  @type error :: {:error, Status.t()}
  @typep method :: :allocateIds | :beginTransaction | :commit | :lookup | :rollback | :runQuery

  @spec allocate_ids(AllocateIdsRequest.t(), Keyword.t) :: list(Key.t()) | error
  @doc "Allocate ids for a list of keys with incomplete key paths"
  def allocate_ids(req, opts \\ nil)
  def allocate_ids(req, opts) do
    req
    |> AllocateIdsRequest.encode()
    |> call(:allocateIds, opts)
    |> case do
      {:ok, body} ->
        body
        |> AllocateIdsResponse.decode()
        |> Key.from_allocate_ids_proto()

      any ->
        any
    end
  end

  @spec commit(CommitRequest.t(), opts :: Keyword.t) :: {:ok, CommitResponse.t()} | error
  @doc "Commit a transaction optionally performing any number of mutations"
  def commit(req, opts \\ nil)
  def commit(req, opts) do
    req
    |> CommitRequest.encode()
    |> call(:commit, opts)
    |> case do
      {:ok, body} -> {:ok, CommitResponse.decode(body)}
      any -> any
    end
  end

  @spec begin_transaction(BeginTransactionRequest.t(), Keyword.t) ::
          {:ok, BeginTransactionResponse.t()} | error
  @doc "Begin a new transaction"
  def begin_transaction(req, opts \\ nil)
  def begin_transaction(req, opts) do
    req
    |> BeginTransactionRequest.encode()
    |> call(:beginTransaction, opts)
    |> case do
      {:ok, body} -> {:ok, BeginTransactionResponse.decode(body)}
      any -> any
    end
  end

  @spec rollback(RollbackRequest.t(), Keyword.t) :: {:ok, RollbackResponse.t()} | error
  @doc "Roll back a transaction specified by a transaction id"
  def rollback(req, opts \\ nil)
  def rollback(req, opts) do
    req
    |> RollbackRequest.encode()
    |> call(:rollback, opts)
    |> case do
      {:ok, body} -> {:ok, RollbackResponse.decode(body)}
      any -> any
    end
  end

  @spec run_query(RunQueryRequest.t(), Keyword.t) :: list(Entity.t()) | error
  @doc "Query for entities"
  def run_query(req, opts \\ nil)
  def run_query(req, opts) do
    req
    |> RunQueryRequest.encode()
    |> call(:runQuery, opts)
    |> case do
      {:ok, body} ->
        result = body |> RunQueryResponse.decode()

        Enum.map(result.batch.entity_results, fn e ->
          Entity.from_proto(e.entity)
        end)

      any ->
        any
    end
  end

  @spec run_query_with_pagination(RunQueryRequest.t(), Keyword.t) :: QueryResultBatch.t() | error
  @doc "Query for entities with pagination metadata"
  def run_query_with_pagination(req, opts \\ nil)
  def run_query_with_pagination(req, opts) do
    req
    |> RunQueryRequest.encode()
    |> call(:runQuery, opts)
    |> case do
      {:ok, body} ->
        body
        |> RunQueryResponse.decode()
        |> Map.get(:batch)
        |> QueryResultBatch.from_proto()

      any ->
        any
    end
  end

  @spec lookup(LookupRequest.t(), opts :: Keyword.t) :: list(Entity.t()) | error
  @doc "Lookup entities by key"
  def lookup(req, opts \\ nil)
  def lookup(req, opts) do
    req
    |> LookupRequest.encode()
    |> call(:lookup, opts)
    |> case do
      {:ok, body} ->
        result = body |> LookupResponse.decode()

        Enum.map(result.found, fn e ->
          Entity.from_proto(e.entity)
        end)

      any ->
        any
    end
  end

  @spec call(String.t(), method(), opts :: Keyword.t) :: {:ok, String.t()} | error | {:error, any}
  defp call(data, method, opts \\ nil) do
    url(method)
    |> HTTPoison.post(data, [auth_header(opts), proto_header()])
    |> case do
      {:ok, %{body: body, status_code: code}} when code in 200..299 ->
        {:ok, body}

      {:ok, %{body: body}} ->
        {:error, Status.decode(body)}

      {:error, %HTTPoison.Error{reason: reason}} ->
        {:error, reason}
    end
  end

  defp url(method, opts), do: url(@api_version, method, opts)

  defp url("v1", method, opts) do
    Path.join([endpoint(opts), @api_version, "projects", "#{project(opts)}:#{method}"])
  end

  defp endpoint, do: Application.get_env(:diplomat, :endpoint, default_endpoint(@api_version))

  defp default_endpoint("v1"), do: "https://datastore.googleapis.com"

  defp token_module(opts \\ nil) do
    cond do
      opts[:token_module] -> opts[:token_module]
      :else -> Application.get_env(:diplomat, :token_module, Goth.Token)
    end
  end

  def goth_project(opts \\ nil) do
    cond do
      opts[:project] -> {:ok, opts[:project]}
      :else -> Goth.Config.get(:project_id)
    end
  end

  defp project(opts \\ nil) do
    {:ok, project_id} = goth_project(opts)
    project_id
  end

  defp api_scope, do: api_scope(@api_version)
  defp api_scope("v1"), do: "https://www.googleapis.com/auth/datastore"

  defp auth_header(opts \\ nil) do
    {:ok, token} = token_module(opts).for_scope(api_scope())
    {"Authorization", "#{token.type} #{token.token}"}
  end

  defp proto_header(_opts \\ nil) do
    {"Content-Type", "application/x-protobuf"}
  end
end
