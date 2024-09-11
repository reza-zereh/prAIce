from pydantic import BaseModel


class SummarizationRequest(BaseModel):
    """
    Represents a request for text summarization.

    Attributes:
        text (str): The input text to be summarized.
        max_tokens (int): The maximum number of tokens in the generated summary.
        model_name (str, optional): The name of the model to be used for summarization.
            Defaults to "facebook/bart-large-cnn".
    """

    text: str
    max_tokens: int = 200
    model_name: str = "facebook/bart-large-cnn"


class SentimentRequest(BaseModel):
    """
    Represents a request for sentiment analysis.

    Attributes:
        text (str): The input text for which the sentiment score needs to be calculated.
        model_name (str, optional): The name of the model to be used for sentiment analysis.
            Defaults to "ProsusAI/finbert".
    """

    text: str
    model_name: str = "ProsusAI/finbert"
