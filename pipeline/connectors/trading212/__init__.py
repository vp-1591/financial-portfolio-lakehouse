"""Trading 212 connector package — registers itself on import."""

from pipeline.connectors.trading212.connector import Trading212Connector

connector = Trading212Connector()