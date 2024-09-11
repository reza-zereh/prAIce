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


def summarize(text, model_name, max_tokens, max_length=3500):
    """
    Summarizes the given text using the specified model.

    Parameters:
        text (str): The input text to be summarized.
        model_name (str): The name of the model to use for summarization.
        max_tokens (int): The maximum number of tokens in the generated summary.
        max_length (int): The maximum length of the input text.

    Returns:
        str: The summary of the input text.
    """
    summarizer = get_summarizer(model_name)
    try:
        return summarizer(
            text[:max_length],
            max_length=max_tokens,
            min_length=max_tokens,
            do_sample=False,
        )[0]["summary_text"]
    except IndexError:
        return summarize(text, model_name, max_tokens, max_length - 100)
