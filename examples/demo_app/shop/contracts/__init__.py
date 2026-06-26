"""Shared contracts — the vocabulary modules agree on.

The contracts module holds the event types that cross module boundaries. A
module that publishes or consumes a cross-module event imports it from here,
so producers and consumers depend on a shared schema rather than on each
other's internals. The boundary verifier treats this module as the sanctioned
place for inter-module types.
"""
