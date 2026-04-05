#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Unit tests for authentication methods."""

from __future__ import annotations

import asyncio
import hashlib

import pytest

from usqldb.net.pgwire._auth import (
    CleartextAuthenticator,
    MD5Authenticator,
    ScramSHA256Authenticator,
    TrustAuthenticator,
    create_authenticator,
)
from usqldb.net.pgwire._errors import InvalidPassword


class TestTrustAuth:
    def test_trust_immediate_success(self):
        auth = TrustAuthenticator("alice", None)
        response, done = asyncio.run(auth.initial())
        assert done is True
        assert response == b""


class TestCleartextAuth:
    def test_cleartext_success(self):
        async def _test():
            auth = CleartextAuthenticator("alice", "secret123")
            response, done = await auth.initial()
            assert done is False
            assert len(response) > 0  # AuthenticationCleartextPassword
            response, done = await auth.step(b"secret123\x00")
            assert done is True

        asyncio.run(_test())

    def test_cleartext_wrong_password(self):
        async def _test():
            auth = CleartextAuthenticator("alice", "secret123")
            await auth.initial()
            with pytest.raises(InvalidPassword):
                await auth.step(b"wrong\x00")

        asyncio.run(_test())

    def test_cleartext_no_password_configured(self):
        async def _test():
            auth = CleartextAuthenticator("alice", None)
            await auth.initial()
            with pytest.raises(InvalidPassword):
                await auth.step(b"anything\x00")

        asyncio.run(_test())


class TestMD5Auth:
    def test_md5_success(self):
        async def _test():
            auth = MD5Authenticator("alice", "secret123")
            response, done = await auth.initial()
            assert done is False
            # Extract salt from the response.
            # Response format: R(1) + length(4) + type=5(4) + salt(4) = 13 bytes
            salt = response[9:13]

            # Compute the expected MD5 hash.
            inner = hashlib.md5(b"secret123alice").hexdigest()
            outer = "md5" + hashlib.md5(inner.encode("utf-8") + salt).hexdigest()

            response, done = await auth.step(outer.encode("utf-8") + b"\x00")
            assert done is True

        asyncio.run(_test())

    def test_md5_wrong_password(self):
        async def _test():
            auth = MD5Authenticator("alice", "secret123")
            await auth.initial()
            with pytest.raises(InvalidPassword):
                await auth.step(b"md5wrong\x00")

        asyncio.run(_test())


class TestScramSHA256Auth:
    def test_scram_full_flow(self):
        """Test the full SCRAM-SHA-256 handshake."""
        import base64
        import hmac as hmac_mod

        async def _test():
            auth = ScramSHA256Authenticator("alice", "secret123")

            # Step 1: Initial -- server sends AuthenticationSASL.
            response, done = await auth.initial()
            assert done is False
            assert b"SCRAM-SHA-256" in response

            # Step 2: Client sends client-first-message.
            client_nonce = "rOprNGfwEbeRWgbNEkqO"
            client_first_bare = f"n=alice,r={client_nonce}"
            client_first_msg = f"n,,{client_first_bare}"

            response, done = await auth.step(client_first_msg.encode("utf-8"))
            assert done is False

            # Parse server-first-message from the SASL continue response.
            # Skip the 'R' header: R(1) + length(4) + type=11(4) = 9 bytes
            server_first = response[9:].decode("utf-8")
            server_attrs: dict[str, str] = {}
            for part in server_first.split(","):
                if "=" in part:
                    key = part[0]
                    val = part[2:]
                    server_attrs[key] = val

            combined_nonce = server_attrs["r"]
            salt = base64.b64decode(server_attrs["s"])
            iterations = int(server_attrs["i"])

            assert combined_nonce.startswith(client_nonce)
            assert len(salt) > 0
            assert iterations > 0

            # Step 3: Client computes proof and sends client-final-message.
            import unicodedata

            password = unicodedata.normalize("NFC", "secret123")
            salted_password = hashlib.pbkdf2_hmac(
                "sha256", password.encode("utf-8"), salt, iterations
            )
            client_key = hmac_mod.new(
                salted_password, b"Client Key", hashlib.sha256
            ).digest()
            stored_key = hashlib.sha256(client_key).digest()
            server_key = hmac_mod.new(
                salted_password, b"Server Key", hashlib.sha256
            ).digest()

            channel_binding = base64.b64encode(b"n,,").decode("ascii")
            client_final_without_proof = f"c={channel_binding},r={combined_nonce}"
            auth_message = (
                f"{client_first_bare},{server_first},{client_final_without_proof}"
            )
            client_signature = hmac_mod.new(
                stored_key, auth_message.encode("utf-8"), hashlib.sha256
            ).digest()
            client_proof = bytes(a ^ b for a, b in zip(client_key, client_signature))
            proof_b64 = base64.b64encode(client_proof).decode("ascii")

            client_final = f"{client_final_without_proof},p={proof_b64}"

            response, done = await auth.step(client_final.encode("utf-8"))
            assert done is True

            # Verify server signature.
            server_sig_expected = hmac_mod.new(
                server_key, auth_message.encode("utf-8"), hashlib.sha256
            ).digest()
            server_final = response[9:].decode("utf-8")
            assert server_final.startswith("v=")
            server_sig_received = base64.b64decode(server_final[2:])
            assert server_sig_received == server_sig_expected

        asyncio.run(_test())


class TestCreateAuthenticator:
    def test_create_trust(self):
        auth = create_authenticator("trust", "alice", None)
        assert isinstance(auth, TrustAuthenticator)

    def test_create_cleartext(self):
        auth = create_authenticator("password", "alice", {"alice": "pw"})
        assert isinstance(auth, CleartextAuthenticator)

    def test_create_md5(self):
        auth = create_authenticator("md5", "alice", {"alice": "pw"})
        assert isinstance(auth, MD5Authenticator)

    def test_create_scram(self):
        auth = create_authenticator("scram-sha-256", "alice", {"alice": "pw"})
        assert isinstance(auth, ScramSHA256Authenticator)

    def test_unknown_method(self):
        with pytest.raises(ValueError, match="Unknown"):
            create_authenticator("kerberos", "alice", None)
