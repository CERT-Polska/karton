import argparse
import getpass

from karton.core.auth.models import AuthClaims, CapabilityClaim, BindClaim

from karton.core.auth.signer import generate_new_keypair

from karton.core.base import KartonBase
from karton.core.asyncio.base import KartonAsyncBase


def generate_keypair(
    passphrase: str | None = None
):
    if not passphrase:
        passphrase = getpass.getpass("Private key passphrase: ")
        confirmation = getpass.getpass("Confirm passphrase: ")

        if not passphrase:
            raise ValueError("An empty passphrase is not permitted")
        if passphrase != confirmation:
            raise ValueError("Passphrases do not match")

    private_pem, jwks = generate_new_keypair(passphrase)


def get_claims_from_karton_class(karton_class: KartonBase|KartonAsyncBase) -> AuthClaims:
    from karton.core import Producer, Consumer, LogConsumer
    from karton.core.asyncio import Producer as AsyncProducer, Consumer as AsyncConsumer

    claims = AuthClaims(identity=karton_class.identity)

    if isinstance(karton_class, (Producer, AsyncProducer)):
        claims.capabilities.append(CapabilityClaim.produce_task)

    if isinstance(karton_class, (Consumer, AsyncConsumer)):
        claims.capabilities.append(CapabilityClaim.consume_task)
        claims.binds.append(BindClaim(
            filters=karton_class.filters,
            persistent=karton_class.persistent,
        ))

    if isinstance(karton_class, (LogConsumer,)):
        claims.capabilities.append(CapabilityClaim.consume_log)

    return claims

def main():
    import argparse
    parser = argparse.ArgumentParser()
