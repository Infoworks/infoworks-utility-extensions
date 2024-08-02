from abc import ABC, abstractmethod


class FileBasedIWXSource(ABC):

    @abstractmethod
    def create_source(self, source_creation_body):
        pass

    @abstractmethod
    def configure_source_connection(self, source_id, source_connection_details_body):
        pass

    @abstractmethod
    def configure_file_mappings(self, source_id, file_mappings_config, reconfigure_tables):
        pass

    @abstractmethod
    def configure_tables(self, source_id, table_id, table_name, table_config, lookup_source_id):
        pass
