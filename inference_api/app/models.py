import torch
from transformers import pipeline


class Summarizer:
    """
    A class representing a summarizer.

    Methods:
        get_instance(model_name): Returns an instance of the summarizer model based on the given model name.
    """

    _instances = {}

    @classmethod
    def get_instance(cls, model_name):
        """
        Get an instance of the specified model.

        Parameters:
            model_name (str): The name of the model.

        Returns:
            The instance of the specified model.
        """
        if model_name not in cls._instances:
            cls._instances[model_name] = pipeline(
                "summarization",
                model=model_name,
                device=torch.device("cuda" if torch.cuda.is_available() else "cpu"),
            )
        return cls._instances[model_name]


def get_summarizer(model_name):
    """
    Returns an instance of the Summarizer class based on the given model name.

    Parameters:
        model_name (str): The name of the model to use for summarization.

    Returns:
        Summarizer: An instance of the Summarizer class.
    """
    return Summarizer.get_instance(model_name)
