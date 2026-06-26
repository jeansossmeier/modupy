"""Built-in plugins.

Each submodule here is a regular plugin against the modulith hookspec
contract. They ship inside the modulith package for convenience but
have no special privileges — users can disable any of them via the
`disable` argument to create_plugin_manager() and replace them with
their own implementations.

This is the architectural test that matters: built-ins are not
privileged, just first-party.
"""
