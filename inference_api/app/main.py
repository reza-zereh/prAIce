from fastapi import FastAPI
from pydantic import BaseModel

from .libs.summarizer import summarize

app = FastAPI()


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


@app.post("/summarize")
async def summarize_route(request: SummarizationRequest):
    """
    Summarizes the given text using a specified model.

    Args:
        request (SummarizationRequest): The request object containing the model name and text to be summarized.

    Returns:
        dict: A dictionary containing the summary of the text.
    """
    summary = summarize(
        text=request.text, model_name=request.model_name, max_tokens=request.max_tokens
    )
    return {"summary": summary}
