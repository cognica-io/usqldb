#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Per-connection state machine for the PostgreSQL wire protocol.

Each TCP connection spawns one :class:`PGWireConnection` which drives
the full protocol lifecycle: startup negotiation, authentication,
simple query, extended query, and graceful termination.
"""

from __future__ import annotations

import asyncio
import logging
import struct
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from usqldb.net.pgwire._auth import (
    AuthMethod,
    create_authenticator,
)
from usqldb.net.pgwire._codec import MessageCodec
from usqldb.net.pgwire._constants import (
    DEFAULT_SERVER_PARAMS,
    FORMAT_BINARY,
    FORMAT_TEXT,
    PROTOCOL_VERSION,
    TX_FAILED,
    TX_IDLE,
    TX_IN_TRANSACTION,
)
from usqldb.net.pgwire._errors import (
    FeatureNotSupported,
    InFailedSQLTransaction,
    PGWireError,
    ProtocolViolation,
    QueryCanceled,
)
from usqldb.net.pgwire._messages import (
    Bind,
    CancelRequest,
    Close,
    ColumnDescription,
    CopyData,
    CopyDone,
    CopyFail,
    Describe,
    Execute,
    Flush,
    FunctionCall,
    GSSENCRequest,
    Parse,
    Query,
    SSLRequest,
    StartupMessage,
    Sync,
    Terminate,
)
from usqldb.net.pgwire._query_executor import QueryExecutor, QueryResult
from usqldb.net.pgwire._type_codec import TypeCodec
from usqldb.pg_compat.connection_registry import (
    ConnectionInfo,
    register as _registry_register,
    unregister as _registry_unregister,
)

if TYPE_CHECKING:
    from usqldb.core.engine import USQLEngine

logger = logging.getLogger("usqldb.pgwire")


# ======================================================================
# Prepared statement / Portal
# ======================================================================


class PreparedStatement:
    """Server-side prepared statement."""

    __slots__ = ("column_descriptions", "name", "param_type_oids", "query")

    def __init__(
        self,
        name: str,
        query: str,
        param_type_oids: list[int],
    ) -> None:
        self.name = name
        self.query = query
        self.param_type_oids = param_type_oids
        self.column_descriptions: list[ColumnDescription] | None = None


class Portal:
    """Bound portal (prepared statement + parameter values)."""

    __slots__ = (
        "name",
        "param_values",
        "result_cache",
        "result_format_codes",
        "row_index",
        "statement",
    )

    def __init__(
        self,
        name: str,
        statement: PreparedStatement,
        param_values: list[Any],
        result_format_codes: list[int],
    ) -> None:
        self.name = name
        self.statement = statement
        self.param_values = param_values
        self.result_format_codes = result_format_codes
        self.result_cache: QueryResult | None = None
        self.row_index = 0


# ======================================================================
# Connection state machine
# ======================================================================


class PGWireConnection:
    """Per-connection protocol handler.

    Created by :class:`PGWireServer` for each accepted TCP connection.
    Call :meth:`run` to drive the full lifecycle.
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        engine: USQLEngine,
        *,
        auth_method: str = AuthMethod.TRUST.value,
        credentials: dict[str, str] | None = None,
        process_id: int = 0,
        secret_key: int = 0,
        cancel_callback: Any = None,
        ssl_context: Any = None,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._engine = engine
        self._executor = QueryExecutor(engine)
        self._auth_method = auth_method
        self._credentials = credentials
        self._process_id = process_id
        self._secret_key = secret_key
        self._cancel_callback = cancel_callback
        self._ssl_context = ssl_context

        self._tx_status = TX_IDLE
        self._statements: dict[str, PreparedStatement] = {}
        self._portals: dict[str, Portal] = {}
        self._session_params: dict[str, str] = dict(DEFAULT_SERVER_PARAMS)
        self._username = ""
        self._database = ""
        self._closed = False
        self._canceled = False

        self._backend_start: datetime | None = None
        self._query_start: datetime | None = None
        self._state_change: datetime | None = None
        self._current_query: str = ""
        self._state: str = "idle"
        self._client_addr: str | None = None
        self._client_port: int = -1
        self._application_name: str = ""

    @property
    def process_id(self) -> int:
        return self._process_id

    @property
    def secret_key(self) -> int:
        return self._secret_key

    def cancel(self) -> None:
        """Mark this connection's current query as canceled."""
        self._canceled = True
        self._engine.cancel()

    # ==================================================================
    # Main lifecycle
    # ==================================================================

    async def run(self) -> None:
        """Drive the connection from startup to termination."""
        try:
            self._backend_start = datetime.now(timezone.utc)
            peername = self._writer.get_extra_info("peername")
            if peername is not None:
                self._client_addr = peername[0]
                self._client_port = peername[1]

            startup = await self._handle_startup()
            if startup is None:
                return  # SSL/Cancel handled, connection closed.

            await self._authenticate(startup)
            await self._send_startup_parameters(startup)
            self._register_connection()
            await self._main_loop()
        except asyncio.IncompleteReadError:
            logger.debug("Client disconnected (pid=%d)", self._process_id)
        except ConnectionResetError:
            logger.debug("Connection reset (pid=%d)", self._process_id)
        except Exception:
            logger.exception("Connection error (pid=%d)", self._process_id)
        finally:
            self._close()

    # ==================================================================
    # Startup phase
    # ==================================================================

    async def _handle_startup(self) -> StartupMessage | None:
        """Read and process startup-phase messages.

        Returns the final :class:`StartupMessage` or ``None`` if the
        connection should be closed (SSL rejected, cancel processed).
        """
        while True:
            # Startup messages have no type byte: 4-byte length + payload.
            raw_len = await self._reader.readexactly(4)
            length = struct.unpack("!I", raw_len)[0]
            payload = await self._reader.readexactly(length - 4)

            msg = MessageCodec.decode_startup(payload)

            if isinstance(msg, SSLRequest):
                if self._ssl_context is not None:
                    self._writer.write(b"S")
                    await self._writer.drain()
                    transport = self._writer.transport
                    protocol = transport.get_protocol()
                    loop = asyncio.get_running_loop()
                    new_transport = await loop.start_tls(
                        transport,
                        protocol,
                        self._ssl_context,
                        server_side=True,
                    )
                    # Replace the underlying transport for TLS.
                    object.__setattr__(self._writer, "_transport", new_transport)
                else:
                    self._writer.write(b"N")
                    await self._writer.drain()
                continue

            if isinstance(msg, GSSENCRequest):
                self._writer.write(b"N")
                await self._writer.drain()
                continue

            if isinstance(msg, CancelRequest):
                if self._cancel_callback is not None:
                    self._cancel_callback(msg.process_id, msg.secret_key)
                return None

            if isinstance(msg, StartupMessage):
                if msg.protocol_version != PROTOCOL_VERSION:
                    err = ProtocolViolation(
                        f"Unsupported protocol version: "
                        f"{msg.protocol_version >> 16}."
                        f"{msg.protocol_version & 0xFFFF}"
                    )
                    await self._send_error(err)
                    return None
                return msg

            return None

    # ==================================================================
    # Authentication
    # ==================================================================

    async def _authenticate(self, startup: StartupMessage) -> None:
        """Run the authentication handshake."""
        self._username = startup.parameters.get("user", "")
        self._database = startup.parameters.get("database", self._username)
        self._application_name = startup.parameters.get("application_name", "")

        auth = create_authenticator(
            self._auth_method, self._username, self._credentials
        )
        response, done = await auth.initial()
        if response:
            self._writer.write(response)
            await self._writer.drain()

        while not done:
            msg_type, payload = await self._read_message()
            if msg_type != ord("p"):
                raise ProtocolViolation(
                    "Expected password/SASL message during authentication"
                )

            # For SCRAM, we need to re-parse the 'p' message.
            if self._auth_method == AuthMethod.SCRAM_SHA_256.value:
                if auth._phase == 0:  # type: ignore[attr-defined]
                    sasl = MessageCodec.decode_sasl_initial_response(payload)
                    response, done = await auth.step(sasl.data)
                else:
                    sasl_r = MessageCodec.decode_sasl_response(payload)
                    response, done = await auth.step(sasl_r.data)
            else:
                response, done = await auth.step(payload)

            if response:
                self._writer.write(response)
                await self._writer.drain()

        # Send AuthenticationOk.
        self._writer.write(MessageCodec.encode_auth_ok())
        await self._writer.drain()

    async def _send_startup_parameters(self, startup: StartupMessage) -> None:
        """Send ParameterStatus, BackendKeyData, and ReadyForQuery."""
        # Merge client-requested parameters.
        if "application_name" in startup.parameters:
            self._session_params["application_name"] = startup.parameters[
                "application_name"
            ]
        if "client_encoding" in startup.parameters:
            self._session_params["client_encoding"] = startup.parameters[
                "client_encoding"
            ]

        # Send all parameter statuses.
        buf = bytearray()
        for name, value in self._session_params.items():
            buf.extend(MessageCodec.encode_parameter_status(name, value))

        # BackendKeyData.
        buf.extend(
            MessageCodec.encode_backend_key_data(self._process_id, self._secret_key)
        )

        # ReadyForQuery.
        buf.extend(MessageCodec.encode_ready_for_query(TX_IDLE))

        self._writer.write(bytes(buf))
        await self._writer.drain()

    # ==================================================================
    # Main message loop
    # ==================================================================

    async def _main_loop(self) -> None:
        """Read and dispatch messages until Terminate or disconnect."""
        while not self._closed:
            msg_type, payload = await self._read_message()
            msg = MessageCodec.decode_frontend(msg_type, payload)

            if isinstance(msg, Query):
                await self._handle_query(msg)
            elif isinstance(msg, Parse):
                await self._handle_parse(msg)
            elif isinstance(msg, Bind):
                await self._handle_bind(msg)
            elif isinstance(msg, Describe):
                await self._handle_describe(msg)
            elif isinstance(msg, Execute):
                await self._handle_execute(msg)
            elif isinstance(msg, Close):
                await self._handle_close(msg)
            elif isinstance(msg, Sync):
                await self._handle_sync()
            elif isinstance(msg, Flush):
                await self._handle_flush()
            elif isinstance(msg, Terminate):
                self._closed = True
            elif isinstance(msg, CopyData):
                pass  # Handled within COPY context
            elif isinstance(msg, CopyDone):
                pass
            elif isinstance(msg, CopyFail):
                pass
            elif isinstance(msg, FunctionCall):
                await self._send_error(
                    FeatureNotSupported("Function call protocol is not supported")
                )
                await self._send_ready_for_query()
            else:
                await self._send_error(
                    ProtocolViolation(f"Unexpected message type: {type(msg).__name__}")
                )

    # ==================================================================
    # Simple query protocol
    # ==================================================================

    async def _handle_query(self, msg: Query) -> None:
        """Handle a simple Query message (potentially multi-statement)."""
        sql = msg.sql.strip()
        if not sql:
            self._writer.write(MessageCodec.encode_empty_query_response())
            await self._send_ready_for_query()
            return

        statements = QueryExecutor.split_statements(sql)
        if not statements:
            self._writer.write(MessageCodec.encode_empty_query_response())
            await self._send_ready_for_query()
            return

        for stmt in statements:
            stmt = stmt.strip()
            if not stmt:
                continue

            if self._canceled:
                self._canceled = False
                await self._send_error(
                    QueryCanceled("canceling statement due to user request")
                )
                self._update_state("idle", "")
                break

            if self._tx_status == TX_FAILED:
                # In a failed transaction, reject all commands except
                # ROLLBACK / COMMIT.
                upper = stmt.strip().upper()
                if not upper.startswith(("ROLLBACK", "COMMIT", "END")):
                    await self._send_error(
                        InFailedSQLTransaction(
                            "current transaction is aborted, "
                            "commands ignored until end of "
                            "transaction block"
                        )
                    )
                    continue

            self._update_state("active", stmt)

            try:
                result = await self._executor.execute(stmt)

                if self._canceled:
                    self._canceled = False
                    await self._send_error(
                        QueryCanceled(
                            "canceling statement due to user request"
                        )
                    )
                    self._update_state("idle", "")
                    break

                await self._send_query_result(result)
            except QueryCanceled as exc:
                self._canceled = False
                await self._send_error(exc)
                self._update_state("idle", "")
                if self._tx_status == TX_IN_TRANSACTION:
                    self._tx_status = TX_FAILED
                break
            except PGWireError as exc:
                await self._send_error(exc)
                if self._tx_status == TX_IN_TRANSACTION:
                    self._tx_status = TX_FAILED
                break
            except Exception as exc:
                from usqldb.net.pgwire._errors import map_engine_exception

                mapped = map_engine_exception(exc)
                await self._send_error(mapped)
                if self._tx_status == TX_IN_TRANSACTION:
                    self._tx_status = TX_FAILED
                break

        self._update_state(
            "idle in transaction"
            if self._tx_status == TX_IN_TRANSACTION
            else "idle",
            "",
        )
        await self._send_ready_for_query()

    async def _send_query_result(self, result: QueryResult) -> None:
        """Send RowDescription + DataRows + CommandComplete for a result."""
        buf = bytearray()

        if result.is_select and result.columns:
            buf.extend(MessageCodec.encode_row_description(result.columns))

            for row in result.rows:
                values: list[bytes | None] = []
                for col_desc in result.columns:
                    val = row.get(col_desc.name)
                    encoded = TypeCodec.encode_text(val, col_desc.type_oid)
                    values.append(encoded)
                buf.extend(MessageCodec.encode_data_row(values))

        buf.extend(MessageCodec.encode_command_complete(result.command_tag))
        self._writer.write(bytes(buf))

    # ==================================================================
    # Extended query protocol
    # ==================================================================

    async def _handle_parse(self, msg: Parse) -> None:
        """Handle Parse -- create a prepared statement."""
        try:
            stmt = PreparedStatement(
                name=msg.statement_name,
                query=msg.query,
                param_type_oids=msg.param_type_oids,
            )

            # Replace unnamed statement and invalidate dependent portals.
            if msg.statement_name == "":
                self._invalidate_portals_for_statement("")
            self._statements[msg.statement_name] = stmt

            self._writer.write(MessageCodec.encode_parse_complete())
        except PGWireError as exc:
            await self._send_error(exc)
        except Exception as exc:
            from usqldb.net.pgwire._errors import map_engine_exception

            await self._send_error(map_engine_exception(exc))

    async def _handle_bind(self, msg: Bind) -> None:
        """Handle Bind -- create a portal from a prepared statement."""
        try:
            stmt = self._statements.get(msg.statement_name)
            if stmt is None:
                raise PGWireError(
                    f'prepared statement "{msg.statement_name}" does not exist'
                )

            # Decode parameter values.
            decoded_params: list[Any] = []
            for i, raw_val in enumerate(msg.param_values):
                if raw_val is None:
                    decoded_params.append(None)
                    continue

                # Determine parameter format.
                if msg.param_format_codes:
                    if len(msg.param_format_codes) == 1:
                        fmt = msg.param_format_codes[0]
                    elif i < len(msg.param_format_codes):
                        fmt = msg.param_format_codes[i]
                    else:
                        fmt = FORMAT_TEXT
                else:
                    fmt = FORMAT_TEXT

                # Determine type OID.
                if i < len(stmt.param_type_oids):
                    oid = stmt.param_type_oids[i]
                else:
                    oid = 0  # unspecified

                if fmt == FORMAT_BINARY and oid != 0:
                    decoded_params.append(TypeCodec.decode_binary(raw_val, oid))
                else:
                    if oid != 0:
                        decoded_params.append(TypeCodec.decode_text(raw_val, oid))
                    else:
                        decoded_params.append(raw_val.decode("utf-8"))

            # Replace unnamed portal.
            if msg.portal_name == "":
                self._portals.pop("", None)

            portal = Portal(
                name=msg.portal_name,
                statement=stmt,
                param_values=decoded_params,
                result_format_codes=msg.result_format_codes,
            )
            self._portals[msg.portal_name] = portal

            self._writer.write(MessageCodec.encode_bind_complete())
        except PGWireError as exc:
            await self._send_error(exc)
        except Exception as exc:
            from usqldb.net.pgwire._errors import map_engine_exception

            await self._send_error(map_engine_exception(exc))

    async def _handle_describe(self, msg: Describe) -> None:
        """Handle Describe -- describe a statement or portal."""
        try:
            if msg.kind == "S":
                stmt = self._statements.get(msg.name)
                if stmt is None:
                    raise PGWireError(f'prepared statement "{msg.name}" does not exist')
                # Send ParameterDescription.
                self._writer.write(
                    MessageCodec.encode_parameter_description(stmt.param_type_oids)
                )

                # Execute to get column descriptions if not cached.
                if stmt.column_descriptions is None:
                    try:
                        result = await self._executor.execute(stmt.query)
                        stmt.column_descriptions = result.columns
                    except Exception:
                        stmt.column_descriptions = []

                if stmt.column_descriptions:
                    self._writer.write(
                        MessageCodec.encode_row_description(stmt.column_descriptions)
                    )
                else:
                    self._writer.write(MessageCodec.encode_no_data())

            elif msg.kind == "P":
                portal = self._portals.get(msg.name)
                if portal is None:
                    raise PGWireError(f'portal "{msg.name}" does not exist')

                # Execute to get column descriptions if not cached.
                if portal.result_cache is None:
                    try:
                        result = await self._executor.execute(
                            portal.statement.query,
                            portal.param_values or None,
                        )
                        portal.result_cache = result
                    except Exception:
                        portal.result_cache = QueryResult([], [], "")

                if portal.result_cache.columns:
                    # Apply result format codes.
                    cols = self._apply_format_codes(
                        portal.result_cache.columns,
                        portal.result_format_codes,
                    )
                    self._writer.write(MessageCodec.encode_row_description(cols))
                else:
                    self._writer.write(MessageCodec.encode_no_data())
            else:
                raise ProtocolViolation(f"Invalid Describe kind: {msg.kind!r}")
        except PGWireError as exc:
            await self._send_error(exc)
        except Exception as exc:
            from usqldb.net.pgwire._errors import map_engine_exception

            await self._send_error(map_engine_exception(exc))

    async def _handle_execute(self, msg: Execute) -> None:
        """Handle Execute -- run a portal."""
        try:
            portal = self._portals.get(msg.portal_name)
            if portal is None:
                raise PGWireError(f'portal "{msg.portal_name}" does not exist')

            # Check transaction state.
            if self._tx_status == TX_FAILED:
                raise InFailedSQLTransaction(
                    "current transaction is aborted, "
                    "commands ignored until end of transaction block"
                )

            self._update_state("active", portal.statement.query)

            # Execute if not cached.
            if portal.result_cache is None:
                result = await self._executor.execute(
                    portal.statement.query,
                    portal.param_values or None,
                )
                portal.result_cache = result

            if self._canceled:
                self._canceled = False
                self._update_state("idle", "")
                raise QueryCanceled(
                    "canceling statement due to user request"
                )

            result = portal.result_cache

            if result.is_select and result.columns:
                # Apply result format codes.
                cols = self._apply_format_codes(
                    result.columns, portal.result_format_codes
                )

                # Determine how many rows to send.
                remaining = result.rows[portal.row_index :]
                if msg.max_rows > 0 and len(remaining) > msg.max_rows:
                    batch = remaining[: msg.max_rows]
                    portal.row_index += msg.max_rows
                    suspended = True
                else:
                    batch = remaining
                    portal.row_index += len(batch)
                    suspended = False

                buf = bytearray()
                for row in batch:
                    values: list[bytes | None] = []
                    for col_desc in cols:
                        val = row.get(col_desc.name)
                        if col_desc.format_code == FORMAT_BINARY:
                            encoded = TypeCodec.encode_binary(val, col_desc.type_oid)
                        else:
                            encoded = TypeCodec.encode_text(val, col_desc.type_oid)
                        values.append(encoded)
                    buf.extend(MessageCodec.encode_data_row(values))

                if suspended:
                    buf.extend(MessageCodec.encode_portal_suspended())
                else:
                    buf.extend(MessageCodec.encode_command_complete(result.command_tag))
                self._writer.write(bytes(buf))
            else:
                self._writer.write(
                    MessageCodec.encode_command_complete(result.command_tag)
                )

            self._update_state(
                "idle in transaction"
                if self._tx_status == TX_IN_TRANSACTION
                else "idle",
                "",
            )

        except QueryCanceled as exc:
            self._canceled = False
            await self._send_error(exc)
            self._update_state("idle", "")
            if self._tx_status == TX_IN_TRANSACTION:
                self._tx_status = TX_FAILED
        except PGWireError as exc:
            await self._send_error(exc)
            if self._tx_status == TX_IN_TRANSACTION:
                self._tx_status = TX_FAILED
        except Exception as exc:
            from usqldb.net.pgwire._errors import map_engine_exception

            await self._send_error(map_engine_exception(exc))
            if self._tx_status == TX_IN_TRANSACTION:
                self._tx_status = TX_FAILED

    async def _handle_close(self, msg: Close) -> None:
        """Handle Close -- close a prepared statement or portal."""
        if msg.kind == "S":
            self._statements.pop(msg.name, None)
            self._invalidate_portals_for_statement(msg.name)
        elif msg.kind == "P":
            self._portals.pop(msg.name, None)
        self._writer.write(MessageCodec.encode_close_complete())

    async def _handle_sync(self) -> None:
        """Handle Sync -- end of extended-query batch."""
        await self._send_ready_for_query()

    async def _handle_flush(self) -> None:
        """Handle Flush -- flush the output buffer."""
        await self._writer.drain()

    # ==================================================================
    # Helpers
    # ==================================================================

    async def _read_message(self) -> tuple[int, bytes]:
        """Read a single typed frontend message."""
        header = await self._reader.readexactly(5)
        msg_type = header[0]
        length = struct.unpack("!I", header[1:5])[0]
        if length < 4:
            raise ProtocolViolation(f"Invalid message length: {length}")
        payload = b""
        if length > 4:
            payload = await self._reader.readexactly(length - 4)
        return msg_type, payload

    async def _send_error(self, error: PGWireError) -> None:
        """Send an ErrorResponse."""
        self._writer.write(MessageCodec.encode_error_response(error.to_fields()))
        await self._writer.drain()

    async def _send_ready_for_query(self) -> None:
        """Send ReadyForQuery with current transaction status."""
        self._writer.write(MessageCodec.encode_ready_for_query(self._tx_status))
        await self._writer.drain()

    def _invalidate_portals_for_statement(self, stmt_name: str) -> None:
        """Remove all portals that reference the given statement."""
        to_remove = [
            name
            for name, portal in self._portals.items()
            if portal.statement.name == stmt_name
        ]
        for name in to_remove:
            del self._portals[name]

    @staticmethod
    def _apply_format_codes(
        columns: list[ColumnDescription],
        format_codes: list[int],
    ) -> list[ColumnDescription]:
        """Apply result format codes to column descriptions."""
        if not format_codes:
            return columns

        result: list[ColumnDescription] = []
        for i, col in enumerate(columns):
            if len(format_codes) == 1:
                fmt = format_codes[0]
            elif i < len(format_codes):
                fmt = format_codes[i]
            else:
                fmt = FORMAT_TEXT
            result.append(col._replace(format_code=fmt))
        return result

    def _update_state(self, state: str, query: str) -> None:
        """Update connection state and sync to the registry."""
        now = datetime.now(timezone.utc)
        self._state = state
        self._current_query = query
        self._state_change = now
        if state == "active":
            self._query_start = now
        self._sync_registry()

    def _register_connection(self) -> None:
        """Register this connection in the global registry."""
        self._sync_registry()

    def _sync_registry(self) -> None:
        """Push current state to the connection registry."""
        _registry_register(
            ConnectionInfo(
                pid=self._process_id,
                username=self._username,
                database=self._database,
                application_name=self._application_name,
                client_addr=self._client_addr,
                client_port=self._client_port,
                backend_start=self._backend_start,
                xact_start=None,
                query_start=self._query_start,
                state_change=self._state_change,
                state=self._state,
                query=self._current_query,
                backend_type="client backend",
            )
        )

    def _close(self) -> None:
        """Clean up connection resources."""
        self._closed = True
        _registry_unregister(self._process_id)
        try:
            if not self._writer.is_closing():
                self._writer.close()
        except Exception:
            pass
