Migration to Karton 6.x
=======================

Version 6.0.0 introduces a new type of backend API called the Karton Gateway API. Karton services communicate via
a Websocket/REST API server (called the Karton Gateway Server) instead of direct calls to the Redis database. Resources
are exchanged via S3 presigned URLs issued by the Karton Gateway, so no direct S3 authentication is needed.

Karton Gateway Server introduces a fine-grained authorization system based on public-key cryptography. It is required
by default, but can be turned off for migration/development purposes.

To allow easier migration:

- Karton service code shouldn't require any changes to migrate to 6.x. The one change you should be aware of
  is non-empty identity requirement, so make sure that all your producers set one. Other breaking changes are
  internal, so they may require attention only if your code operates directly on the Karton Backend.
- Karton services are still able to connect directly to Redis and S3, using so-called "direct backend".
  This option will be turned off in future versions.

How to migrate from 5.x
-----------------------

1. Ensure that all your services are running ``karton-core`` in at least v5.0.0 version.
   v6 drops the compatibility with v4 release.

2. Set up the Karton Gateway server. See :ref:`gateway-setup` section of Getting started for instructions.

3. Upgrade ``karton-system`` instances to the v6.0.0. Also upgrade ``karton-dashboard`` if it is used in your
   environment.

4. Then for each service:

   - upgrade it to the version compatible with Karton v6.0.0 and check if it boots correctly with direct access
     to Redis and S3

   - apply the Gateway server configuration as described in :ref:`service-configuration` section of Getting started

   - restart it with new configuration and check if it runs correctly

   If there is no available v6 version, you can try to workaround it by enforcing the installation of Karton v6
   disregarding the ``<6.0.0`` Karton version pins

5. Finally you can cut off the Redis access for the Karton services, leaving it only for Karton System and Karton
   Gateway. Karton Dashboard also works with the Gateway backend, when configured with a properly authorized
   token for the Gateway inspection API.

Things to be aware of while upgrading custom services
-----------------------------------------------------

In usual cases, Karton services built on high-level ``Producer``/``Consumer``/``Karton`` classes don't require any
changes other than setting a non-empty identity. But if your service operates directly on the Karton Backend,
there are a few things you should check:

- Backend instances should be created using :func:`get_backend <karton.core.backend.get_backend>` factory,
  which returns the right implementation based on the configuration
  (``KartonGatewayBackend`` for Gateway, ``KartonBackend`` for direct access).
- Identity must be always set to a non-empty string without spaces and ``?`` characters, otherwise
  ``InvalidIdentityError`` exception is raised.


See :ref:`breaking-changes-6-0` for the complete list of changes introduced in v6.0.0.
