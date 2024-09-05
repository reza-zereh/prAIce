import os
from abc import ABC, abstractmethod
from typing import Literal

import torch
from anthropic import Anthropic
from openai import OpenAI
from transformers import pipeline


class Summarizer(ABC):
    """
    Abstract base class for summarizers.
    """

    @abstractmethod
    def summarize(self, text: str, max_tokens: int) -> str:
        """
        Abstract method for summarizing text.

        Parameters:
            text (str): The input text to be summarized.
            max_tokens (int): The maximum number of tokens in the summary.

        Returns:
            str: The summarized text.
        """
        pass


class AnthropicSummarizer(Summarizer):
    """
    Summarizer class that uses the Anthropic API.
    """

    def __init__(self, model_name: str = "claude-3-5-sonnet-20240620"):
        self.client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
        self.model = model_name

    def summarize(self, text: str, max_tokens: int) -> str:
        """
        Summarizes the input text using the Anthropic API.

        Parameters:
            text (str): The input text to be summarized.
            max_tokens (int): The maximum number of tokens in the summary.

        Returns:
            str: The summarized text.
        """
        response = self.client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            messages=[
                {
                    "role": "user",
                    "content": f"Please summarize the following text in about {max_tokens} tokens and in one paragraph:\n\n{text}",
                }
            ],
        )
        return response.content[0].text


class OpenAISummarizer(Summarizer):
    """
    Summarizer class that uses the OpenAI API.
    """

    def __init__(self, model_name: str = "gpt-4o-mini"):
        self.client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        self.model = model_name

    def summarize(self, text: str, max_tokens: int) -> str:
        """
        Summarizes the input text using the OpenAI API.

        Parameters:
            text (str): The input text to be summarized.
            max_tokens (int): The maximum number of tokens in the summary.

        Returns:
            str: The summarized text.
        """
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful assistant that summarizes text.",
                },
                {
                    "role": "user",
                    "content": f"Please summarize the following text in about {max_tokens} tokens and in one paragraph:\n\n{text}",
                },
            ],
            max_tokens=max_tokens,
        )
        return response.choices[0].message["content"].strip()


class HuggingFaceSummarizer(Summarizer):
    """
    Summarizer class that uses the Hugging Face Transformers library.
    """

    def __init__(self, model_name: str):
        self.pipeline = pipeline(
            "summarization",
            model=model_name,
            device=torch.device("cuda" if torch.cuda.is_available() else "cpu"),
        )

    def summarize(self, text: str, max_tokens: int) -> str:
        """
        Summarizes the input text using the Hugging Face Transformers library.

        Parameters:
            text (str): The input text to be summarized.
            max_tokens (int): The maximum number of tokens in the summary.

        Returns:
            str: The summarized text.
        """
        results = self.pipeline(
            text[:3500],
            max_length=max_tokens,
            min_length=max_tokens,
            do_sample=False,
        )

        return results[0]["summary_text"]


class SummarizerFactory:
    """
    Factory class for creating summarizers based on the API type.

    Methods:
        get_summarizer(api_type: Literal["anthropic", "openai", "bart"]) -> Summarizer:
            Returns a summarizer instance based on the provided API type.

    Attributes:
        _summarizers: A dictionary to store created summarizer instances.
    """

    _summarizers = {}

    @classmethod
    def get_summarizer(
        cls, api_type: Literal["anthropic", "openai", "bart"]
    ) -> Summarizer:
        """
        Returns a summarizer based on the specified API type.

        Parameters:
            api_type (Literal["anthropic", "openai", "bart"]): The API type to determine the summarizer.

        Returns:
            Summarizer: The summarizer object based on the specified API type.

        Raises:
            ValueError: If the API type is invalid. Choose 'anthropic' or 'openai'.
        """
        if api_type not in cls._summarizers:
            match api_type:
                case "anthropic":
                    cls._summarizers[api_type] = AnthropicSummarizer(
                        model_name="claude-3-5-sonnet-20240620"
                    )
                case "openai":
                    cls._summarizers[api_type] = OpenAISummarizer(
                        model_name="gpt-4o-mini"
                    )
                case "bart":
                    cls._summarizers[api_type] = HuggingFaceSummarizer(
                        model_name="facebook/bart-large-cnn"
                    )
                case _:
                    raise ValueError(
                        "Invalid API type. Choose 'anthropic' or 'openai'."
                    )
        return cls._summarizers[api_type]
