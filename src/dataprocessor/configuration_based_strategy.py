import io
import json
from abc import ABC, abstractmethod
from uuid import uuid4


class AbstractProcessor(ABC):
    def __init__(self, trace_id, layer_data):
        # Assign a trace id for distributed tracing and for facilitating rollback (in the event of a failure)
        self._trace_id = trace_id

        self._data = layer_data

    @abstractmethod
    def _process(self):
        """
        Process method is expected to process the data SYNCHRONOUSLY when is_independent_of_children=True
        Otherwise, the implementation of this method would trigger a async call
        :return:
        """
        pass

    def _notify(self):
        """
        This method can be used to notify a subscriber upon completion of processing.
        :return:
        """
        pass

    @abstractmethod
    def _undo_process(self):
        pass

    def process_data(self):
        self._process()


class AbstractStrategy(ABC):
    """
    This interface (abstract class in Python) contains method defs that should be implemented by any strategy
    """

    @abstractmethod
    def validate(self):
        """
        :brief: The purpose of this method is to ensure the following:
                1. Validate if the layer have all the necessary (mandatory) fields present
                2. Validate if the layer doesn't have a field or a sub-layer that isn't allowed

            In the event of validation failure, the method raises an InvalidInputException
        :return:
        :raises: InvalidInputException when validation fails
        """
        pass

    def process(self):
        """
        Process the current level / layer data
        :return:
        """
        pass

    def store(self):
        """
        Store the processed results
        :return:
        """
        pass


class AbstractTransformer(ABC):
    @abstractmethod
    def validate(self):
        pass

    @abstractmethod
    def transform(self):
        pass


class RuleBasedTransformer(AbstractTransformer):
    """
    Fetches the transformation rule from a pre-configured source like a file / DB and performs the transformation
    """

    def validate(self):
        pass

    def transform(self):
        pass


class ReflectiveTransformer(AbstractTransformer):
    """
    Performs validation and transformation based on pre-existing transformer classes written for a specific data type
    """

    def validate(self):
        pass

    def transform(self):
        pass


class AbstractNotifier(ABC):
    @abstractmethod
    def notify(self):
        pass


class NullNotifier(AbstractNotifier):
    def notify(self):
        # No action is needed
        pass


class EmailNotifier(AbstractNotifier):
    def notify(self):
        pass


class MessageQueueNotifier(AbstractNotifier):
    def notify(self):
        pass


class AbstractStorage(ABC):
    @abstractmethod
    def store(self):
        pass

    @abstractmethod
    def remove(self):


class StoreToS3(AbstractStorage):
    pass


class StoreToFile(AbstractStorage):
    pass


class StoreToDB(AbstractStorage):
    pass


class TransformerFactory(object):
    @staticmethod
    def get_transformer(data):
        if data.field_name in ('address', 'transaction'):
            return RuleBasedTransformer
        else:
            return ReflectiveTransformer


class NotifierFactory(object):
    @staticmethod
    def get_notifier(data):
        if data.field_name in ('address',):
            return EmailNotifier
        elif data.field_name in ('location', 'transaction'):
            return MessageQueueNotifier
        else:
            return NullNotifier


class StorageFactory(object):
    @staticmethod
    def get_storage(data):
        if data.field_name in ('address',):
            return StoreToS3
        elif data.field_name in ('location', 'transaction'):
            return StoreToDB
        else:
            return StoreToFile


class ConcreteStrategy(AbstractStrategy):
    def __init__(self, data, stop_on_failure=True, is_independent_of_children=False):
        # This attribute defines whether the current level / layer can be processed independently without having
        # to process the nested levels.
        self.is_independent_of_children = is_independent_of_children

        # Let the processor decide whether the data parsing should be stopped (and other changes should be
        # reverted back in the event of a failure)
        self.stop_on_failure = stop_on_failure
        self.transformer = TransformerFactory.get_transformer(data)()
        self.storage_handler = StorageFactory.get_storage(data)()
        self.notifier_obj = NotifierFactory.get_notifier(data)()

    def validate(self):
        self.transformer.validate()

    def process(self):
        self.transformer.transform()
        self.notifier_obj.notify()

    def store(self):
        self.storage_handler.store()


class ConcreteProcessor(AbstractProcessor):

    def __init__(self, trace_id, layer_data):
        super().__init__(trace_id=trace_id, layer_data=layer_data)
        self.strategy = ConcreteStrategy(layer_data)

    def _process(self):
        layer_nodes = []
        # Process data fields

        # Capture node fields and queue it up for processing
        for field, value in self._data.items():
            if type(value) is dict:
                layer_nodes.append(value)

        processor_obj = None
        for layer_node in layer_nodes:
            try:
                processor_obj = ConcreteProcessor(self._trace_id, layer_node)
                if processor_obj.strategy.is_independent_of_children:
                    # Process the data in async fashion with a callback / future
                    # The callback ensures the completion
                    pass
                processor_obj.process_data()
            except InvalidInputException:
                if processor_obj.strategy_obj.stop_on_failure:
                    processor_obj.notify()
                    exit(-1)


class Driver(object):
    def get_content_from_file(self, file_path):
        """
        Reading the entire file as the maximum file size would be only 15 MB.
        On my laptop (16 GB RAM) reading a 24 MB file took only took ~ 1 second
        :param file_path:
        :return:
        """
        with open(file_path, 'r') as file_obj:
            fi = io.FileIO(file_obj.fileno())
            fo = io.BufferedReader(fi)
            content = fo.read()
            return content

    def main(self, file_path):
        content = self.get_content_from_file(file_path)
        layer_data = json.loads(content)
        trace_id = uuid4()
        processor_obj = ConcreteProcessor(trace_id=trace_id, layer_data=layer_data)
        processor_obj.process_data()
