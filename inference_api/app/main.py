from fastapi import FastAPI
from pydantic import BaseModel

from .models import get_summarizer

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
    max_tokens: int
    model_name: str = "facebook/bart-large-cnn"


@app.post("/summarize")
async def summarize(request: SummarizationRequest):
    """
    Summarizes the given text using a specified model.

    Args:
        request (SummarizationRequest): The request object containing the model name and text to be summarized.

    Returns:
        dict: A dictionary containing the summary of the text.
    """
    summarizer = get_summarizer(request.model_name)
    summary = summarizer(
        request.text[:3400],
        max_length=request.max_tokens,
        min_length=request.max_tokens,
        do_sample=False,
    )
    return {"summary": summary[0]["summary_text"]}
