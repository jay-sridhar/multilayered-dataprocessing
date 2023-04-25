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


class AbstractAddressStrategy(ABC):
    """
    This interface (abstract class in Python) contains method defs that should be implemented by an address strategy
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


class BaseAddressStrategy(AbstractAddressStrategy):
    def __init__(self, stop_on_failure=True, is_independent_of_children=False):
        # This attribute defines whether the current level / layer can be processed independently without having
        # to process the nested levels.
        self.is_independent_of_children = is_independent_of_children

        # Let the processor decide whether the data parsing should be stopped (and other changes should be
        # reverted back in the event of a failure)
        self.stop_on_failure = stop_on_failure

    def validate(self):
        pass

    def process(self):
        pass

    def store(self):
        pass


class DefaultAddressStrategy(BaseAddressStrategy):

    def process(self):
        pass


class StreetAddressStrategy(BaseAddressStrategy):
    def __init__(self):
        super().__init__(stop_on_failure=False, is_independent_of_children=True)

    def validate(self):
        super().validate()
        # Additional validation pertaining to street

    def process(self):
        super().process()
        # Additional post processing


class AvenueAddressStrategy(BaseAddressStrategy):
    def validate(self):
        super().validate()
        # Additional validation pertaining to avenue

    def process(self):
        super().process()
        # Additional post processing


class AddressProcessor(AbstractProcessor):
    def __init__(self, trace_id, layer_data):
        super().__init__(trace_id=trace_id, layer_data=layer_data)
        self.strategy = StrategyResolver.get_strategy(layer_data.field_name, layer_data.data)

    def _validate(self):
        self.strategy(self._data).validate()

    def _process(self):
        self.strategy(self._data).process()


class StrategyResolver(object):
    @staticmethod
    def get_strategy(field_name, data):
        if field_name == 'address':
            if 'street' in data:
                return StreetAddressStrategy
            elif 'avenue' in data:
                return AvenueAddressStrategy
            else:
                return DefaultAddressStrategy


class ConcreteProcessor(AbstractProcessor):

    def __init__(self, trace_id, layer_data):
        super().__init__(trace_id=trace_id, layer_data=layer_data)
        self.strategy_obj = StrategyResolver.get_strategy(layer_data.field_name, layer_data.data)

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
                if processor_obj.strategy_obj.is_independent_of_children:
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
