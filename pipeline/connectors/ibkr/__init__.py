"""IBKR connector package — registers itself on import."""

from pipeline.connectors.ibkr.connector import IbkrConnector

connector = IbkrConnector()